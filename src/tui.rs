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
//! - **Stats box**: the same numbers the periodic stats log line reports, plus overall data
//!   rate from the CAR byte counter.
//! - **Log pane**: most recent log lines, which raw-mode rendering would otherwise garble.
//!
//! Press `q`, `Esc`, or `Ctrl-C` to request the same graceful shutdown as `SIGINT`.

use std::collections::VecDeque;
use std::io::Stdout;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers, MouseButton,
    MouseEventKind,
};
use crossterm::{execute, terminal};
use jetstreamer_firehose::firehose::OP_TIMEOUT;
use jetstreamer_firehose::node_reader::TOTAL_BYTES_READ;
use jetstreamer_plugin::metrics;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph};

const RENDER_INTERVAL: Duration = Duration::from_millis(250);
const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const LOG_BUFFER_CAP: usize = 300;

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

static LOG_LINES: Mutex<VecDeque<(log::Level, String)>> = Mutex::new(VecDeque::new());

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
        let mut lines = LOG_LINES.lock().unwrap();
        if lines.len() >= LOG_BUFFER_CAP {
            lines.pop_front();
        }
        lines.push_back((
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

struct RateSampler {
    last_sample: Instant,
    last_txs: u64,
    last_bytes: u64,
    /// One TPS sample per [`SAMPLE_INTERVAL`] covering the whole run.
    tps_history: Vec<f64>,
    bytes_per_sec: f64,
}

impl RateSampler {
    fn new() -> Self {
        Self {
            last_sample: Instant::now(),
            last_txs: 0,
            last_bytes: 0,
            tps_history: Vec::new(),
            bytes_per_sec: 0.0,
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
        self.last_sample = Instant::now();
    }

    /// Samples within the selected trailing window (`None` = the whole run).
    fn windowed(&self, window_secs: Option<u64>) -> &[f64] {
        match window_secs {
            None => &self.tps_history,
            Some(secs) => {
                let samples = (secs as f64 / SAMPLE_INTERVAL.as_secs_f64()) as usize;
                let start = self.tps_history.len().saturating_sub(samples.max(1));
                &self.tps_history[start..]
            }
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

fn render_loop(stop: Arc<AtomicBool>) {
    let mut terminal = match setup_terminal() {
        Ok(terminal) => terminal,
        Err(err) => {
            eprintln!("failed to initialize TUI terminal: {err}");
            return;
        }
    };
    let mut sampler = RateSampler::new();
    let mut selected_range = DEFAULT_RANGE;
    let mut hitboxes: Vec<RangeHitbox> = Vec::new();

    while !stop.load(Ordering::SeqCst) {
        drain_input(&mut selected_range, &hitboxes);
        sampler.maybe_sample();
        let _ = terminal.draw(|frame| {
            hitboxes = draw(frame, &sampler, selected_range);
        });
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
/// drives the same graceful shutdown (raw mode swallows the real Ctrl-C signal). Range labels
/// respond to left click and the number keys `1`–`7`.
fn drain_input(selected_range: &mut usize, hitboxes: &[RangeHitbox]) {
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
                } else if let KeyCode::Char(digit @ '1'..='7') = key.code {
                    *selected_range = digit as usize - '1' as usize;
                }
            }
            Event::Mouse(mouse) => {
                if mouse.kind == MouseEventKind::Down(MouseButton::Left) {
                    for &(x_start, x_end, y, index) in hitboxes {
                        if mouse.row == y && mouse.column >= x_start && mouse.column < x_end {
                            *selected_range = index;
                            break;
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

fn draw(
    frame: &mut ratatui::Frame,
    sampler: &RateSampler,
    selected_range: usize,
) -> Vec<RangeHitbox> {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Percentage(35),
            Constraint::Percentage(38),
            Constraint::Min(4),
        ])
        .split(frame.area());
    let middle = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Min(30), Constraint::Length(42)])
        .split(rows[2]);

    let hitboxes = draw_range_selector(frame, rows[0], selected_range);
    draw_tps_chart(frame, rows[1], sampler, selected_range);
    draw_thread_grid(frame, middle[0]);
    draw_stats(frame, middle[1], sampler);
    draw_logs(frame, rows[3]);
    hitboxes
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
    let history = sampler.windowed(window_secs);
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
        .block(Block::default().borders(Borders::ALL).title(format!(
            " TPS ({label}) — current {} | peak {} ",
            human_count(current as u64),
            human_count(peak as u64),
        )))
        .x_axis(
            Axis::default()
                .bounds([0.0, 1.0])
                .labels::<Vec<Span>>(vec![Span::raw(start_label), "now".into()]),
        )
        .y_axis(
            Axis::default()
                .bounds([0.0, peak.max(1.0) * 1.1])
                .labels::<Vec<Span>>(vec![
                    "0".into(),
                    Span::raw(human_count((peak * 1.1) as u64)),
                ]),
        );
    frame.render_widget(chart, area);
}

fn draw_thread_grid(frame: &mut ratatui::Frame, area: Rect) {
    let timeout_ms = OP_TIMEOUT.as_millis() as u64;
    let thread_count = metrics::thread_count();
    let mut active = 0usize;
    let cols = ((area.width as usize).saturating_sub(3) / 2).max(1);
    let mut lines: Vec<Line> = Vec::new();
    let mut row: Vec<Span> = Vec::new();
    for thread_id in 0..thread_count {
        let (symbol, color) = match metrics::thread_idle_ms(thread_id) {
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
    let title = format!(
        " Threads — {active}/{thread_count} active (green <{:.1}s yellow <{:.1}s orange <{:.1}s red ≥{:.1}s) ",
        timeout_ms as f64 / 10.0 / 1000.0,
        timeout_ms as f64 / 2.0 / 1000.0,
        timeout_ms as f64 / 1000.0,
        timeout_ms as f64 / 1000.0,
    );
    let grid = Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(title));
    frame.render_widget(grid, area);
}

fn draw_stats(frame: &mut ratatui::Frame, area: Rect, sampler: &RateSampler) {
    let pulse = metrics::latest_pulse().unwrap_or_default();
    let total_bytes = TOTAL_BYTES_READ.load(Ordering::Relaxed);
    let stat = |label: &str, value: String| -> Line {
        Line::from(vec![
            Span::styled(
                format!("{label:>10}: "),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(value),
        ])
    };
    let lines = vec![
        stat("progress", format!("{:.1}%", pulse.progress_pct)),
        stat("ETA", pulse.eta.clone().unwrap_or_else(|| "n/a".into())),
        stat("TPS", human_count(pulse.tps.ceil() as u64)),
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

fn draw_logs(frame: &mut ratatui::Frame, area: Rect) {
    let capacity = (area.height as usize).saturating_sub(2);
    let lines: Vec<Line> = {
        let buffer = LOG_LINES.lock().unwrap();
        buffer
            .iter()
            .rev()
            .take(capacity)
            .rev()
            .map(|(level, message)| {
                let color = match level {
                    log::Level::Error => Color::Red,
                    log::Level::Warn => Color::Yellow,
                    log::Level::Info => Color::Reset,
                    _ => Color::DarkGray,
                };
                Line::from(Span::styled(message.clone(), Style::default().fg(color)))
            })
            .collect()
    };
    let logs = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Logs — q/Esc/Ctrl-C to stop "),
    );
    frame.render_widget(logs, area);
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
