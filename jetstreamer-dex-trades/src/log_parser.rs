//! Log parser for extracting events from Solana transaction logs.
//!
//! Maps `Program log:` and `Program data:` messages to their instruction
//! positions, enabling event-based decoding for DEXes like Raydium AMM V4
//! and Anchor-style programs.

use std::collections::HashMap;

#[derive(Debug, Clone)]
struct ProgramContext {
    program_id: String,
    #[allow(dead_code)]
    depth: u32,
    instruction_position: String,
}

struct PositionTracker {
    outer_index: usize,
    current_inner_count: usize,
}

impl PositionTracker {
    fn new() -> Self {
        Self {
            outer_index: 0,
            current_inner_count: 0,
        }
    }

    fn on_invoke(&mut self, depth: u32) -> String {
        if depth == 1 {
            self.outer_index += 1;
            self.current_inner_count = 0;
            return self.outer_index.to_string();
        }
        self.current_inner_count += 1;
        format!("{}.{}", self.outer_index, self.current_inner_count)
    }
}

/// Log parser for extracting events by instruction position.
pub struct LogParser;

impl LogParser {
    /// Extract `Program log:` messages grouped by instruction position.
    pub fn extract_program_logs_by_position(
        logs: &[String],
        target_program: &str,
    ) -> HashMap<String, Vec<String>> {
        let mut result: HashMap<String, Vec<String>> = HashMap::new();
        let mut stack: Vec<ProgramContext> = Vec::new();
        let mut tracker = PositionTracker::new();

        for log in logs {
            if let Some((program_id, depth)) = Self::parse_invoke(log) {
                let position = tracker.on_invoke(depth);
                stack.push(ProgramContext {
                    program_id,
                    depth,
                    instruction_position: position,
                });
            } else if let Some(program_id) = Self::parse_completion(log) {
                if let Some(pos) = stack.iter().rposition(|ctx| ctx.program_id == program_id) {
                    stack.remove(pos);
                }
            } else if let Some(log_msg) = log.strip_prefix("Program log: ")
                && let Some(ctx) = stack.last()
                && ctx.program_id == target_program
            {
                result
                    .entry(ctx.instruction_position.clone())
                    .or_default()
                    .push(log_msg.to_string());
            }
        }

        result
    }

    /// Extract `Program data:` events grouped by instruction position.
    pub fn extract_program_data_by_position(
        logs: &[String],
        target_program: &str,
    ) -> HashMap<String, Vec<String>> {
        let mut result: HashMap<String, Vec<String>> = HashMap::new();
        let mut stack: Vec<ProgramContext> = Vec::new();
        let mut tracker = PositionTracker::new();

        for log in logs {
            if let Some((program_id, depth)) = Self::parse_invoke(log) {
                let position = tracker.on_invoke(depth);
                stack.push(ProgramContext {
                    program_id,
                    depth,
                    instruction_position: position,
                });
            } else if let Some(program_id) = Self::parse_completion(log) {
                if let Some(pos) = stack.iter().rposition(|ctx| ctx.program_id == program_id) {
                    stack.remove(pos);
                }
            } else if let Some(data) = log.strip_prefix("Program data: ")
                && let Some(ctx) = stack.last()
                && ctx.program_id == target_program
            {
                result
                    .entry(ctx.instruction_position.clone())
                    .or_default()
                    .push(data.to_string());
            }
        }

        result
    }

    /// Returns true if any log line contains "Log truncated".
    pub fn is_truncated(logs: &[String]) -> bool {
        logs.iter().any(|l| l.contains("Log truncated"))
    }

    fn parse_invoke(log: &str) -> Option<(String, u32)> {
        if log.starts_with("Program ") && log.contains(" invoke [") && log.ends_with(']') {
            let parts: Vec<&str> = log.split_whitespace().collect();
            if parts.len() >= 4 && parts[0] == "Program" && parts[2] == "invoke" {
                let program_id = parts[1].to_string();
                let depth_str = parts[3].trim_start_matches('[').trim_end_matches(']');
                if let Ok(depth) = depth_str.parse::<u32>() {
                    return Some((program_id, depth));
                }
            }
        }
        None
    }

    fn parse_completion(log: &str) -> Option<String> {
        if log.starts_with("Program ") {
            let parts: Vec<&str> = log.split_whitespace().collect();
            if parts.len() >= 3
                && parts[0] == "Program"
                && (parts[2] == "success" || parts[2].starts_with("failed"))
            {
                return Some(parts[1].to_string());
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_program_logs() {
        let logs = vec![
            "Program 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 invoke [1]".to_string(),
            "Program log: ray_log: SGVsbG8=".to_string(),
            "Program 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 success".to_string(),
        ];

        let result = LogParser::extract_program_logs_by_position(
            &logs,
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        );

        assert_eq!(
            result.get("1"),
            Some(&vec!["ray_log: SGVsbG8=".to_string()])
        );
    }

    #[test]
    fn test_nested_cpi() {
        let logs = vec![
            "Program JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4 invoke [1]".to_string(),
            "Program 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 invoke [2]".to_string(),
            "Program log: ray_log: first_swap".to_string(),
            "Program 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 success".to_string(),
            "Program 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 invoke [2]".to_string(),
            "Program log: ray_log: second_swap".to_string(),
            "Program 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8 success".to_string(),
            "Program JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4 success".to_string(),
        ];

        let result = LogParser::extract_program_logs_by_position(
            &logs,
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        );

        assert_eq!(
            result.get("1.1"),
            Some(&vec!["ray_log: first_swap".to_string()])
        );
        assert_eq!(
            result.get("1.2"),
            Some(&vec!["ray_log: second_swap".to_string()])
        );
    }
}
