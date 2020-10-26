use crate::default_logger;
use datadriven::{run_test, TestData};

fn test_confchange(data: &TestData) -> String {}

#[test]
fn test_data_driven_confchange() -> anyhow::Result<()> {
    let logger = default_logger();
    run_test("src/confchange/testdata", test_confchange, false, &logger)?;
    Ok(())
}
