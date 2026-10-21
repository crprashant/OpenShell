// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "e2e-kubernetes")]

//! A sandbox whose pod the Kubernetes scheduler cannot place reports why it is
//! waiting: `sandbox create` shows the scheduler reason, and `sandbox get` /
//! `sandbox list` expose it after the create command has exited.

use std::process::Stdio;
use std::time::{Duration, Instant};

use openshell_e2e::harness::binary::openshell_cmd;
use openshell_e2e::harness::cli::run_cli;
use openshell_e2e::harness::output::strip_ansi;

/// Far beyond any CI or developer node, so the scheduler can never fit it.
const UNSCHEDULABLE_MEMORY: &str = "64Ti";
const CREATE_IDLE_TIMEOUT_SECS: &str = "45";

struct DeleteOnDrop(String);

impl Drop for DeleteOnDrop {
    fn drop(&mut self) {
        let _ = std::process::Command::new(openshell_e2e::harness::binary::openshell_bin())
            .args(["sandbox", "delete", &self.0])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn scheduling_condition(sandbox: &serde_json::Value) -> Option<&serde_json::Value> {
    sandbox["conditions"]
        .as_array()?
        .iter()
        .find(|condition| condition["type"] == "PodScheduled" && condition["status"] == "False")
}

async fn sandbox_json(name: &str) -> Option<serde_json::Value> {
    let (output, code) = run_cli(&["sandbox", "get", name, "--output", "json"]).await;
    if code != 0 {
        return None;
    }
    serde_json::from_str(&strip_ansi(&output)).ok()
}

#[tokio::test]
async fn unschedulable_sandbox_reports_scheduler_reason() {
    // Sandbox names are capped at 19 characters.
    let name = format!(
        "e2e-sched-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis()
            % 1_000_000_000
    );
    let _cleanup = DeleteOnDrop(name.clone());

    let mut create = openshell_cmd()
        .env("OPENSHELL_PROVISION_TIMEOUT", CREATE_IDLE_TIMEOUT_SECS)
        .args([
            "sandbox",
            "create",
            "--name",
            &name,
            "--memory",
            UNSCHEDULABLE_MEMORY,
            "--no-tty",
            "--",
            "true",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn sandbox create");

    // While create waits, the scheduling reason is observable from another client.
    let deadline = Instant::now() + Duration::from_mins(2);
    let condition = loop {
        if let Some(sandbox) = sandbox_json(&name).await
            && let Some(condition) = scheduling_condition(&sandbox)
        {
            assert_eq!(sandbox["phase"], "Provisioning", "sandbox: {sandbox}");
            break condition.clone();
        }
        if create.try_wait().expect("poll sandbox create").is_some() {
            let output = create.wait_with_output().await.expect("create output");
            panic!(
                "sandbox create exited before reporting a scheduling wait:\n{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        assert!(
            Instant::now() < deadline,
            "sandbox {name} never reported PodScheduled=False: {:?}",
            sandbox_json(&name).await
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    let message = condition["message"].as_str().unwrap_or_default();
    assert_eq!(
        condition["reason"], "Unschedulable",
        "condition: {condition}"
    );
    assert!(
        message.contains("Insufficient memory"),
        "scheduler message should explain the blocker: {condition}"
    );

    let (list, code) = run_cli(&["sandbox", "list"]).await;
    let list = strip_ansi(&list);
    assert_eq!(code, 0, "sandbox list failed:\n{list}");
    assert!(
        list.lines()
            .any(|line| line.contains(&name) && line.contains("Provisioning (Unschedulable)")),
        "sandbox list should mark the unschedulable sandbox:\n{list}"
    );

    let (detail, code) = run_cli(&["sandbox", "get", &name]).await;
    let detail = strip_ansi(&detail);
    assert_eq!(code, 0, "sandbox get failed:\n{detail}");
    assert!(
        detail.contains("Scheduling: Unschedulable:") && detail.contains("Insufficient memory"),
        "sandbox get should show the scheduling reason:\n{detail}"
    );

    // The wait does not extend the idle deadline, so create gives up and says why.
    let output = tokio::time::timeout(Duration::from_mins(5), create.wait_with_output())
        .await
        .expect("sandbox create should time out on its own")
        .expect("sandbox create output");
    let combined = strip_ansi(&format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ));
    assert!(
        !output.status.success(),
        "create must not succeed:\n{combined}"
    );
    assert!(
        combined.contains("Waiting to be scheduled"),
        "create should show the scheduling wait:\n{combined}"
    );
    assert!(
        combined.contains("timed out") && combined.contains("Insufficient memory"),
        "create timeout should carry the scheduler reason:\n{combined}"
    );
}
