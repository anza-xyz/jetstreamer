//! Interactive terminal dashboard rendered when the CLI runs with `--tui`.
//!
//! Layout:
//! - **Range selector**: clickable time ranges (`5m`–`12h` show the trailing window, `all`
//!   spans the entire run and compresses as it grows). Number keys `1`–`7` also select.
//! - **TPS graph** over the selected range.
//! - **Thread grid**: one dot per firehose thread, colored by how recently data flowed
//!   through it. Thresholds are percentages of the firehose operation timeout: green under
//!   10%, yellow under 50%, orange under 100%, red at or beyond the timeout (stalled or
//!   backing off). Gray dots have not reported data yet.
//! - **System chart**: overall CPU %, memory %, and download bandwidth on a shared 0–100%
//!   axis (bandwidth normalized to its in-window peak; the title states what 100% equals).
//! - **Stats box**: the same numbers the periodic stats log line reports, plus overall data
//!   rate and per-thread avg/min/max TPS.
//! - **Log pane** (full width): recent log lines. Scroll with the mouse wheel over the pane
//!   or PageUp/PageDown; End (or scrolling to the bottom) resumes following new lines.
//!
//! Pane dividers are mouse-draggable: grab the border between the graph and the middle row,
//! between the middle row and the logs, or between the thread grid and the stats box.
//!
//! Press `q`, `Esc`, or `Ctrl-C` to request the same graceful shutdown as `SIGINT`.

use std::collections::VecDeque;
use std::io::Stdout;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers, MouseButton, MouseEvent,
    MouseEventKind,
};
use crossterm::{execute, terminal};
use jetstreamer_firehose::firehose::{OP_TIMEOUT, thread_activity};
use jetstreamer_firehose::node_reader::TOTAL_BYTES_READ;
use jetstreamer_plugin::metrics;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::Margin;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols;
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph, Scrollbar, ScrollbarOrientation,
    ScrollbarState,
};

const RENDER_INTERVAL: Duration = Duration::from_millis(250);
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const LOG_BUFFER_CAP: usize = 2000;
const LOG_SCROLL_STEP: usize = 3;

/// Selectable graph windows: label plus trailing seconds (`None` = entire run).
const TIME_RANGES: [(&str, Option<u64>); 7] = [
    ("5m", Some(300)),
    ("15m", Some(900)),
    ("30m", Some(1800)),
    ("1h", Some(3600)),
    ("6h", Some(21600)),
    ("12h", Some(43200)),
    ("all", None),
];
const DEFAULT_RANGE: usize = TIME_RANGES.len() - 1;

/// Captured log lines plus a count of lines evicted from the front, so scroll anchors can be
/// expressed as stable absolute line numbers even as the ring buffer rotates.
struct LogBuffer {
    lines: VecDeque<(log::Level, String)>,
    dropped: u64,
}

impl LogBuffer {
    /// Absolute index one past the newest line.
    fn total(&self) -> u64 {
        self.dropped + self.lines.len() as u64
    }

    /// Smallest valid viewport end for a viewport of `capacity` lines.
    fn min_end(&self, capacity: usize) -> u64 {
        self.dropped + self.lines.len().min(capacity) as u64
    }
}

static LOG_LINES: Mutex<LogBuffer> = Mutex::new(LogBuffer {
    lines: VecDeque::new(),
    dropped: 0,
});

/// `log::Log` implementation that captures records into a ring buffer for the log pane
/// instead of writing to the terminal (which raw-mode rendering would garble).
struct RingLogger {
    max_level: log::LevelFilter,
}

impl log::Log for RingLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= self.max_level
    }

    fn log(&self, record: &log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        // Mirror the CLI default of capping clickhouse client chatter at warn.
        if record.target().starts_with("clickhouse") && record.level() > log::Level::Warn {
            return;
        }
        let mut buffer = LOG_LINES.lock().unwrap();
        if buffer.lines.len() >= LOG_BUFFER_CAP {
            buffer.lines.pop_front();
            buffer.dropped += 1;
        }
        buffer.lines.push_back((
            record.level(),
            format!("{} {}", record.target(), record.args()),
        ));
    }

    fn flush(&self) {}
}

/// Installs the ring-buffer logger. Call instead of `solana_logger` setup when the TUI owns
/// the terminal. `level` accepts the leading level token of a filter string like `"info"`.
pub fn init_logging(level: &str) {
    let max_level = level
        .split(',')
        .next()
        .and_then(|token| token.trim().parse::<log::LevelFilter>().ok())
        .unwrap_or(log::LevelFilter::Info);
    if log::set_boxed_logger(Box::new(RingLogger { max_level })).is_ok() {
        log::set_max_level(max_level);
    }
}

/// Prints the most recent `count` captured log lines to stderr. Called after the dashboard
/// exits: the ring logger stays installed for the rest of the process, so this keeps final
/// errors and the run summary visible in the terminal scrollback.
pub fn dump_recent_logs(count: usize) {
    let buffer = LOG_LINES.lock().unwrap();
    for (level, message) in buffer.lines.iter().rev().take(count).rev() {
        eprintln!("[{level}] {message}");
    }
}

/// Handle to the background render thread; restores the terminal on [`TuiHandle::stop`] (or
/// drop, as a best-effort backstop).
pub struct TuiHandle {
    stop: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl TuiHandle {
    /// Signals the render thread to exit and restores the terminal.
    pub fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for TuiHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Starts the dashboard render thread. The terminal is switched to raw mode and the
/// alternate screen until the returned handle is stopped or dropped.
pub fn start() -> TuiHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_flag = stop.clone();
    let join = std::thread::Builder::new()
        .name("jetstreamer-tui".into())
        .spawn(move || render_loop(stop_flag))
        .expect("failed to spawn TUI thread");
    TuiHandle {
        stop,
        join: Some(join),
    }
}

/// Per-thread TPS aggregate across threads that have reported data: `(avg, min, max)`.
type ThreadTpsAggregate = (f64, f64, f64);

struct RateSampler {
    last_sample: Instant,
    last_txs: u64,
    last_bytes: u64,
    last_thread_txs: Vec<u64>,
    /// One TPS sample per [`SAMPLE_INTERVAL`] covering the whole run.
    tps_history: Vec<f64>,
    /// Overall CPU usage percent, sampled alongside `tps_history`.
    cpu_history: Vec<f64>,
    /// Overall memory usage percent, sampled alongside `tps_history`.
    mem_history: Vec<f64>,
    /// Download bandwidth in bytes/sec, sampled alongside `tps_history`.
    net_history: Vec<f64>,
    bytes_per_sec: f64,
    thread_tps: Option<ThreadTpsAggregate>,
    system: sysinfo::System,
}

impl RateSampler {
    fn new() -> Self {
        Self {
            last_sample: Instant::now(),
            last_txs: 0,
            last_bytes: 0,
            last_thread_txs: Vec::new(),
            tps_history: Vec::new(),
            cpu_history: Vec::new(),
            mem_history: Vec::new(),
            net_history: Vec::new(),
            bytes_per_sec: 0.0,
            thread_tps: None,
            system: sysinfo::System::new(),
        }
    }

    fn maybe_sample(&mut self) {
        let elapsed = self.last_sample.elapsed();
        if elapsed < SAMPLE_INTERVAL {
            return;
        }
        let secs = elapsed.as_secs_f64();
        let txs = metrics::latest_pulse()
            .map(|pulse| pulse.transactions_processed)
            .unwrap_or(0);
        let bytes = TOTAL_BYTES_READ.load(Ordering::Relaxed);
        self.tps_history
            .push(txs.saturating_sub(self.last_txs) as f64 / secs);
        self.bytes_per_sec = bytes.saturating_sub(self.last_bytes) as f64 / secs;
        self.last_txs = txs;
        self.last_bytes = bytes;
        self.sample_thread_tps(secs);
        self.sample_system();
        self.last_sample = Instant::now();
    }

    fn sample_system(&mut self) {
        self.system.refresh_cpu_usage();
        self.system.refresh_memory();
        self.cpu_history.push(self.system.global_cpu_usage() as f64);
        let total_mem = self.system.total_memory();
        let mem_pct = if total_mem > 0 {
            self.system.used_memory() as f64 / total_mem as f64 * 100.0
        } else {
            0.0
        };
        self.mem_history.push(mem_pct);
        self.net_history.push(self.bytes_per_sec);
    }

    /// Computes avg/min/max TPS across threads that have processed any transactions.
    fn sample_thread_tps(&mut self, secs: f64) {
        let thread_count = metrics::thread_count();
        self.last_thread_txs.resize(thread_count, 0);
        let mut rates: Vec<f64> = Vec::with_capacity(thread_count);
        for (thread_id, last) in self.last_thread_txs.iter_mut().enumerate() {
            let total = metrics::thread_tx_count(thread_id);
            if total > 0 {
                rates.push(total.saturating_sub(*last) as f64 / secs);
            }
            *last = total;
        }
        self.thread_tps = if rates.is_empty() {
            None
        } else {
            let sum: f64 = rates.iter().sum();
            let min = rates.iter().copied().fold(f64::INFINITY, f64::min);
            let max = rates.iter().copied().fold(0.0_f64, f64::max);
            Some((sum / rates.len() as f64, min, max))
        };
    }
}

/// Slice of `history` covering the selected trailing window (`None` = the whole run).
fn window_slice(history: &[f64], window_secs: Option<u64>) -> &[f64] {
    match window_secs {
        None => history,
        Some(secs) => {
            let samples = (secs as f64 / SAMPLE_INTERVAL.as_secs_f64()) as usize;
            let start = history.len().saturating_sub(samples.max(1));
            &history[start..]
        }
    }
}

/// Averages `history` down to at most `buckets` points with x normalized to `[0, 1]`, so the
/// graph always spans exactly the selected range.
fn downsample(history: &[f64], buckets: usize) -> Vec<(f64, f64)> {
    if history.is_empty() || buckets == 0 {
        return Vec::new();
    }
    let per_bucket = history.len().div_ceil(buckets);
    let total = history.len() as f64;
    history
        .chunks(per_bucket)
        .enumerate()
        .map(|(i, chunk)| {
            let x = (i * per_bucket) as f64 / total;
            let avg = chunk.iter().sum::<f64>() / chunk.len() as f64;
            (x, avg)
        })
        .collect()
}

/// Hit-test target for a clickable range label: `(x_start, x_end_exclusive, y, range_index)`.
type RangeHitbox = (u16, u16, u16, usize);

/// Divider currently being dragged.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DragTarget {
    /// Horizontal divider between the TPS chart and the middle row.
    ChartMiddle,
    /// Horizontal divider between the middle row and the log pane.
    MiddleLogs,
    /// Vertical divider between the system chart and the stats box.
    SystemStats,
}

/// Mutable UI state shared between input handling and drawing.
struct UiState {
    selected_range: usize,
    range_hitboxes: Vec<RangeHitbox>,
    /// Chart height as a percentage of the frame height.
    chart_pct: u16,
    /// Middle row (threads + stats) height as a percentage of the frame height.
    middle_pct: u16,
    /// Stats box width in columns.
    stats_width: u16,
    drag: Option<DragTarget>,
    /// Absolute line number of the log viewport's bottom edge while scrolled; `None` follows
    /// new lines. Anchoring to an absolute position keeps the text frozen for reading while
    /// new lines continue to arrive.
    log_anchor: Option<u64>,
    /// Clickable "back to live" button in the log pane title: `(x_start, x_end_exclusive, y)`.
    live_button: Option<(u16, u16, u16)>,
    /// Forces a full clear + repaint on the next frame (initially, and again on resize).
    needs_clear: bool,
    // Last-frame geometry for hit-testing.
    frame_area: Rect,
    chart_rect: Rect,
    middle_rect: Rect,
    logs_rect: Rect,
    grid_rect: Rect,
    system_rect: Rect,
}

impl UiState {
    fn new() -> Self {
        Self {
            selected_range: DEFAULT_RANGE,
            range_hitboxes: Vec::new(),
            chart_pct: 35,
            middle_pct: 38,
            stats_width: 42,
            drag: None,
            log_anchor: None,
            live_button: None,
            needs_clear: true,
            frame_area: Rect::default(),
            chart_rect: Rect::default(),
            middle_rect: Rect::default(),
            logs_rect: Rect::default(),
            grid_rect: Rect::default(),
            system_rect: Rect::default(),
        }
    }
}

fn render_loop(stop: Arc<AtomicBool>) {
    let mut terminal = match setup_terminal() {
        Ok(terminal) => terminal,
        Err(err) => {
            eprintln!("failed to initialize TUI terminal: {err}");
            return;
        }
    };
    let mut sampler = RateSampler::new();
    let mut state = UiState::new();

    while !stop.load(Ordering::SeqCst) {
        drain_input(&mut state);
        if state.needs_clear {
            // Full clear + repaint: wipes whatever was on screen before startup (a
            // multiplexer without altscreen support — `screen` defaults to `altscreen off` —
            // draws over live scrollback) and repairs stale cells after a resize.
            let _ = terminal.clear();
            state.needs_clear = false;
        }
        sampler.maybe_sample();
        let _ = terminal.draw(|frame| draw(frame, &sampler, &mut state));
        std::thread::sleep(RENDER_INTERVAL);
    }

    restore_terminal(&mut terminal);
}

fn setup_terminal() -> std::io::Result<Terminal<CrosstermBackend<Stdout>>> {
    terminal::enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(
        stdout,
        terminal::EnterAlternateScreen,
        EnableMouseCapture,
        crossterm::cursor::Hide
    )?;
    Terminal::new(CrosstermBackend::new(stdout))
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) {
    let _ = terminal::disable_raw_mode();
    let _ = execute!(
        terminal.backend_mut(),
        DisableMouseCapture,
        terminal::LeaveAlternateScreen,
        crossterm::cursor::Show
    );
}

/// Processes pending input. Quit keys raise `SIGINT` so the runner's existing Ctrl-C handler
/// drives the same graceful shutdown (raw mode swallows the real Ctrl-C signal).
fn drain_input(state: &mut UiState) {
    while crossterm::event::poll(Duration::ZERO).unwrap_or(false) {
        let Ok(event) = crossterm::event::read() else {
            return;
        };
        match event {
            Event::Key(key) => {
                let ctrl_c =
                    key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL);
                if ctrl_c || matches!(key.code, KeyCode::Char('q') | KeyCode::Esc) {
                    unsafe {
                        libc::raise(libc::SIGINT);
                    }
                } else {
                    handle_key(state, key.code);
                }
            }
            Event::Mouse(mouse) => handle_mouse(state, mouse),
            Event::Resize(..) => state.needs_clear = true,
            _ => {}
        }
    }
}

fn handle_key(state: &mut UiState, code: KeyCode) {
    let log_capacity = log_view_capacity(state);
    match code {
        KeyCode::Char(digit @ '1'..='7') => {
            state.selected_range = digit as usize - '1' as usize;
        }
        KeyCode::PageUp => scroll_logs(state, log_capacity as i64),
        KeyCode::PageDown => scroll_logs(state, -(log_capacity as i64)),
        KeyCode::Up => scroll_logs(state, 1),
        KeyCode::Down => scroll_logs(state, -1),
        KeyCode::Home => {
            let buffer = LOG_LINES.lock().unwrap();
            state.log_anchor = Some(buffer.min_end(log_capacity));
        }
        KeyCode::End => state.log_anchor = None,
        _ => {}
    }
}

fn log_view_capacity(state: &UiState) -> usize {
    (state.logs_rect.height as usize).saturating_sub(2).max(1)
}

/// Scrolls the log viewport by `delta` lines (positive = toward older lines). While scrolled,
/// the viewport anchors to an absolute line number so arriving lines don't move the text;
/// scrolling back to the newest line resumes following.
fn scroll_logs(state: &mut UiState, delta: i64) {
    let capacity = log_view_capacity(state);
    let buffer = LOG_LINES.lock().unwrap();
    let total = buffer.total();
    let current_end = state.log_anchor.unwrap_or(total).min(total);
    let new_end = current_end
        .saturating_add_signed(-delta)
        .clamp(buffer.min_end(capacity), total);
    state.log_anchor = if new_end >= total {
        None
    } else {
        Some(new_end)
    };
}

fn handle_mouse(state: &mut UiState, mouse: MouseEvent) {
    match mouse.kind {
        MouseEventKind::Down(MouseButton::Left) => {
            for &(x_start, x_end, y, index) in &state.range_hitboxes {
                if mouse.row == y && mouse.column >= x_start && mouse.column < x_end {
                    state.selected_range = index;
                    return;
                }
            }
            if let Some((x_start, x_end, y)) = state.live_button
                && mouse.row == y
                && mouse.column >= x_start
                && mouse.column < x_end
            {
                state.log_anchor = None;
                return;
            }
            state.drag = grab_divider(state, mouse.column, mouse.row);
        }
        MouseEventKind::Drag(MouseButton::Left) => {
            if let Some(target) = state.drag {
                drag_divider(state, target, mouse.column, mouse.row);
            }
        }
        MouseEventKind::Up(MouseButton::Left) => state.drag = None,
        MouseEventKind::ScrollUp if rect_contains(state.logs_rect, mouse.column, mouse.row) => {
            scroll_logs(state, LOG_SCROLL_STEP as i64);
        }
        MouseEventKind::ScrollDown if rect_contains(state.logs_rect, mouse.column, mouse.row) => {
            scroll_logs(state, -(LOG_SCROLL_STEP as i64));
        }
        _ => {}
    }
}

fn rect_contains(rect: Rect, x: u16, y: u16) -> bool {
    x >= rect.x && x < rect.x + rect.width && y >= rect.y && y < rect.y + rect.height
}

/// Identifies which divider (if any) a press at `(x, y)` grabs. Each divider is the pair of
/// adjacent border lines between two panes.
fn grab_divider(state: &UiState, x: u16, y: u16) -> Option<DragTarget> {
    let chart_bottom = state.chart_rect.y + state.chart_rect.height.saturating_sub(1);
    if y == chart_bottom || y == state.middle_rect.y {
        return Some(DragTarget::ChartMiddle);
    }
    let middle_bottom = state.middle_rect.y + state.middle_rect.height.saturating_sub(1);
    if y == middle_bottom || y == state.logs_rect.y {
        return Some(DragTarget::MiddleLogs);
    }
    let in_middle_rows =
        y >= state.middle_rect.y && y < state.middle_rect.y + state.middle_rect.height;
    let system_right = state.system_rect.x + state.system_rect.width.saturating_sub(1);
    if (x == system_right || x == system_right + 1) && in_middle_rows {
        return Some(DragTarget::SystemStats);
    }
    None
}

fn drag_divider(state: &mut UiState, target: DragTarget, x: u16, y: u16) {
    let total_height = state.frame_area.height.max(1) as u32;
    let pct_of_height = |row: u16| -> u16 {
        ((row.saturating_sub(state.frame_area.y) as u32 * 100) / total_height) as u16
    };
    match target {
        DragTarget::ChartMiddle => {
            // Keep the logs boundary fixed: the middle row absorbs what the chart gives up.
            let total_top = state.chart_pct + state.middle_pct;
            let chart_pct = pct_of_height(y).clamp(10, total_top.saturating_sub(10));
            state.chart_pct = chart_pct;
            state.middle_pct = total_top - chart_pct;
        }
        DragTarget::MiddleLogs => {
            let total_top = pct_of_height(y).clamp(state.chart_pct + 10, 90);
            state.middle_pct = total_top - state.chart_pct;
        }
        DragTarget::SystemStats => {
            let middle_right = state.middle_rect.x + state.middle_rect.width;
            let width = middle_right.saturating_sub(x);
            let max_width = state.middle_rect.width.saturating_sub(20);
            state.stats_width = width.clamp(24, max_width.max(24));
        }
    }
}

fn draw(frame: &mut ratatui::Frame, sampler: &RateSampler, state: &mut UiState) {
    state.frame_area = frame.area();
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Percentage(state.chart_pct),
            Constraint::Percentage(state.middle_pct),
            Constraint::Min(3),
        ])
        .split(frame.area());
    // Size the thread grid to its content — enough columns (2 cells per dot) to fit every
    // thread in the rows available — and give the system chart all remaining width.
    let grid_rows = (rows[2].height as usize).saturating_sub(2).max(1);
    let thread_count = metrics::thread_count().max(1);
    let grid_width = (thread_count.div_ceil(grid_rows) * 2 + 3) as u16;
    let middle = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(grid_width.clamp(12, rows[2].width / 2)),
            Constraint::Min(20),
            Constraint::Length(state.stats_width),
        ])
        .split(rows[2]);
    state.chart_rect = rows[1];
    state.middle_rect = rows[2];
    state.logs_rect = rows[3];
    state.grid_rect = middle[0];
    state.system_rect = middle[1];

    state.range_hitboxes = draw_range_selector(frame, rows[0], state.selected_range);
    draw_tps_chart(frame, rows[1], sampler, state.selected_range);
    draw_thread_grid(frame, middle[0]);
    draw_system_chart(frame, middle[1], sampler, state.selected_range);
    draw_stats(frame, middle[2], sampler);
    draw_logs(frame, rows[3], state);
}

fn draw_range_selector(
    frame: &mut ratatui::Frame,
    area: Rect,
    selected_range: usize,
) -> Vec<RangeHitbox> {
    let mut spans: Vec<Span> = vec![Span::raw(" range: ")];
    let mut hitboxes = Vec::with_capacity(TIME_RANGES.len());
    let mut x = area.x + " range: ".len() as u16;
    for (index, (label, _)) in TIME_RANGES.iter().enumerate() {
        let text = format!("[{label}]");
        let width = text.len() as u16;
        let style = if index == selected_range {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD | Modifier::REVERSED)
        } else {
            Style::default().fg(Color::Cyan)
        };
        spans.push(Span::styled(text, style));
        spans.push(Span::raw(" "));
        hitboxes.push((x, x + width, area.y, index));
        x += width + 1;
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
    hitboxes
}

fn draw_tps_chart(
    frame: &mut ratatui::Frame,
    area: Rect,
    sampler: &RateSampler,
    selected_range: usize,
) {
    let (label, window_secs) = TIME_RANGES[selected_range];
    let history = window_slice(&sampler.tps_history, window_secs);
    let points = downsample(
        history,
        (area.width as usize).saturating_sub(10).max(10) * 2,
    );
    let current = history.last().copied().unwrap_or(0.0);
    let peak = history.iter().copied().fold(0.0_f64, f64::max);
    let covered_secs = history.len() as f64 * SAMPLE_INTERVAL.as_secs_f64();
    let dataset = Dataset::default()
        .marker(symbols::Marker::Braille)
        .graph_type(GraphType::Line)
        .style(Style::default().fg(Color::Cyan))
        .data(&points);
    let start_label = if window_secs.is_some() {
        format!("-{}", human_duration(covered_secs))
    } else {
        "start".to_string()
    };
    let chart = Chart::new(vec![dataset])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" TPS ({label}) ")),
        )
        .x_axis(
            Axis::default()
                .bounds([0.0, 1.0])
                .labels::<Vec<Span>>(vec![Span::raw(start_label), "now".into()]),
        )
        .y_axis(
            // Readout axis rather than a true scale: 0 at the bottom, the current rate as
            // the middle label, and the window peak at the top (the top bound is exactly the
            // peak, so that label is also positionally accurate).
            Axis::default()
                .bounds([0.0, peak.max(1.0)])
                .labels::<Vec<Span>>(vec![
                    "0".into(),
                    Span::raw(human_count(current as u64)),
                    Span::raw(human_count(peak as u64)),
                ]),
        );
    frame.render_widget(chart, area);
}

fn draw_thread_grid(frame: &mut ratatui::Frame, area: Rect) {
    let timeout_ms = OP_TIMEOUT.as_millis() as u64;
    let thread_count = metrics::thread_count();
    let mut active = 0usize;
    let mut done = 0usize;
    let cols = ((area.width as usize).saturating_sub(3) / 2).max(1);
    let mut lines: Vec<Line> = Vec::new();
    let mut row: Vec<Span> = Vec::new();
    for thread_id in 0..thread_count {
        // A finished thread stops reading forever; without the explicit marker its idle
        // clock would paint it red as if stalled.
        let (symbol, color) = if thread_activity::is_finished(thread_id) {
            done += 1;
            ("✓", Color::Rgb(0, 190, 190))
        } else {
            match metrics::thread_idle_ms(thread_id) {
                None => ("·", Color::DarkGray),
                Some(idle_ms) => {
                    if idle_ms < timeout_ms {
                        active += 1;
                    }
                    if idle_ms < timeout_ms / 10 {
                        ("●", Color::Green)
                    } else if idle_ms < timeout_ms / 2 {
                        ("●", Color::Yellow)
                    } else if idle_ms < timeout_ms {
                        ("●", Color::Rgb(255, 140, 0))
                    } else {
                        ("●", Color::Red)
                    }
                }
            }
        };
        row.push(Span::styled(symbol, Style::default().fg(color)));
        row.push(Span::raw(" "));
        if row.len() / 2 >= cols {
            lines.push(Line::from(std::mem::take(&mut row)));
        }
    }
    if !row.is_empty() {
        lines.push(Line::from(row));
    }
    let title = format!(" Threads {active}/{thread_count} ✓{done} ");
    let grid = Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(title));
    frame.render_widget(grid, area);
}

/// CPU %, memory %, and download bandwidth on one chart. Everything shares the 0–100% axis:
/// CPU and memory are natively percentages, and bandwidth is scaled so 100% is the highest
/// rate observed anywhere in the run — the title states what 100% equals, and the legend
/// shows each line's live absolute value in its color.
fn draw_system_chart(
    frame: &mut ratatui::Frame,
    area: Rect,
    sampler: &RateSampler,
    selected_range: usize,
) {
    let (_, window_secs) = TIME_RANGES[selected_range];
    let buckets = (area.width as usize).saturating_sub(10).max(10) * 2;
    let cpu = downsample(window_slice(&sampler.cpu_history, window_secs), buckets);
    let mem = downsample(window_slice(&sampler.mem_history, window_secs), buckets);
    let net_abs = downsample(window_slice(&sampler.net_history, window_secs), buckets);
    // Full scale is the run's highest observed rate (not the window's), so trailing windows
    // don't renormalize themselves to always touch 100%.
    let net_full_scale = sampler
        .net_history
        .iter()
        .copied()
        .fold(0.0_f64, f64::max)
        .max(1.0);
    let net: Vec<(f64, f64)> = net_abs
        .iter()
        .map(|&(x, v)| (x, (v / net_full_scale * 100.0).min(100.0)))
        .collect();

    let cpu_now = sampler.cpu_history.last().copied().unwrap_or(0.0);
    let mem_now = sampler.mem_history.last().copied().unwrap_or(0.0);
    let datasets = vec![
        Dataset::default()
            .marker(symbols::Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Magenta))
            .data(&cpu),
        Dataset::default()
            .marker(symbols::Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::LightBlue))
            .data(&mem),
        Dataset::default()
            .marker(symbols::Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Green))
            .data(&net),
    ];
    // Legend lives in the title, colored to match each line, because the chart widget's
    // built-in legend hides itself when it decides the area is too small.
    let title = Line::from(vec![
        Span::raw(" System "),
        Span::styled(
            format!("cpu {cpu_now:.0}%"),
            Style::default().fg(Color::Magenta),
        ),
        Span::raw(" | "),
        Span::styled(
            format!("mem {mem_now:.0}%"),
            Style::default().fg(Color::LightBlue),
        ),
        Span::raw(" | "),
        Span::styled(
            format!("net {}/s", human_bytes(sampler.bytes_per_sec)),
            Style::default().fg(Color::Green),
        ),
        Span::raw(format!(" (100% = {}/s) ", human_bytes(net_full_scale))),
    ]);
    let chart = Chart::new(datasets)
        .block(Block::default().borders(Borders::ALL).title(title))
        .x_axis(Axis::default().bounds([0.0, 1.0]))
        .y_axis(
            Axis::default()
                .bounds([0.0, 100.0])
                .labels::<Vec<Span>>(vec!["0%".into(), "100%".into()]),
        );
    frame.render_widget(chart, area);
}

fn draw_stats(frame: &mut ratatui::Frame, area: Rect, sampler: &RateSampler) {
    let pulse = metrics::latest_pulse().unwrap_or_default();
    let total_bytes = TOTAL_BYTES_READ.load(Ordering::Relaxed);
    let stat = |label: &str, value: String| -> Line {
        Line::from(vec![
            Span::styled(
                format!("{label:>11}: "),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(value),
        ])
    };
    let (avg_tps, min_tps, max_tps) = match sampler.thread_tps {
        Some((avg, min, max)) => (
            human_count(avg as u64),
            human_count(min as u64),
            human_count(max as u64),
        ),
        None => ("n/a".into(), "n/a".into(), "n/a".into()),
    };
    let lines = vec![
        stat("progress", format!("{:.1}%", pulse.progress_pct)),
        stat("ETA", pulse.eta.clone().unwrap_or_else(|| "n/a".into())),
        stat("TPS", human_count(pulse.tps.ceil() as u64)),
        stat("thr TPS avg", avg_tps),
        stat("thr TPS min", min_tps),
        stat("thr TPS max", max_tps),
        stat(
            "slots",
            format!(
                "{} / {}",
                human_count(pulse.slots_processed),
                human_count(pulse.total_slots)
            ),
        ),
        stat("blocks", human_count(pulse.blocks_processed)),
        stat("txs", human_count(pulse.transactions_processed)),
        stat("entries", human_count(pulse.entries_processed)),
        stat("rewards", human_count(pulse.rewards_processed)),
        stat(
            "data rate",
            format!("{}/s", human_bytes(sampler.bytes_per_sec)),
        ),
        stat("data total", human_bytes(total_bytes as f64)),
        stat("elapsed", human_duration(pulse.elapsed_secs)),
    ];
    let stats =
        Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(" Stats "));
    frame.render_widget(stats, area);
}

fn draw_logs(frame: &mut ratatui::Frame, area: Rect, state: &mut UiState) {
    let capacity = (area.height as usize).saturating_sub(2).max(1);
    let paused = state.log_anchor.is_some();
    let (lines, start_index, line_count) = {
        let buffer = LOG_LINES.lock().unwrap();
        let total = buffer.total();
        let end_abs = state
            .log_anchor
            .map(|anchor| anchor.clamp(buffer.min_end(capacity), total))
            .unwrap_or(total);
        let end = (end_abs - buffer.dropped) as usize;
        let start = end.saturating_sub(capacity);
        let lines: Vec<Line> = buffer
            .lines
            .iter()
            .skip(start)
            .take(end - start)
            .map(|(level, message)| {
                let color = match level {
                    log::Level::Error => Color::Red,
                    log::Level::Warn => Color::Yellow,
                    log::Level::Info => Color::Reset,
                    _ => Color::DarkGray,
                };
                Line::from(Span::styled(message.clone(), Style::default().fg(color)))
            })
            .collect();
        (lines, start, buffer.lines.len())
    };

    // Title with a clickable live/paused indicator; its span position doubles as the hitbox.
    let prefix = " Logs ";
    let button_text = if paused {
        "[▶ back to live]"
    } else {
        "[● live]"
    };
    let button_style = if paused {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Green)
    };
    let hint = " wheel/PgUp scroll — q/Esc/Ctrl-C to stop ";
    let button_x = area.x + 1 + prefix.chars().count() as u16;
    state.live_button = Some((
        button_x,
        button_x + button_text.chars().count() as u16,
        area.y,
    ));
    let title = Line::from(vec![
        Span::raw(prefix),
        Span::styled(button_text, button_style),
        Span::raw(hint),
    ]);
    let logs = Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(title));
    frame.render_widget(logs, area);

    if line_count > capacity {
        let mut scrollbar_state = ScrollbarState::new(line_count.saturating_sub(capacity))
            .viewport_content_length(capacity)
            .position(start_index);
        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight),
            area.inner(Margin {
                horizontal: 0,
                vertical: 1,
            }),
            &mut scrollbar_state,
        );
    }
}

fn human_count(value: u64) -> String {
    match value {
        0..=9_999 => value.to_string(),
        10_000..=999_999 => format!("{:.1}k", value as f64 / 1e3),
        1_000_000..=999_999_999 => format!("{:.2}M", value as f64 / 1e6),
        _ => format!("{:.2}B", value as f64 / 1e9),
    }
}

fn human_bytes(bytes: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes.max(0.0);
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

fn human_duration(secs: f64) -> String {
    let secs = secs.max(0.0) as u64;
    let (hours, minutes, seconds) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    if hours > 0 {
        format!("{hours}h{minutes:02}m{seconds:02}s")
    } else if minutes > 0 {
        format!("{minutes}m{seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}
