use crate::Plugin;

pub struct ProgramTrackingPlugin {}

impl Plugin for ProgramTrackingPlugin {
    fn name(&self) -> &'static str {
        "Program Tracking"
    }

    fn on_transaction(
        &mut self,
        db: &clickhouse::Client,
        transaction: crate::bridge::Transaction,
    ) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    fn on_block(
        &mut self,
        db: &clickhouse::Client,
        block: crate::bridge::Block,
    ) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}
