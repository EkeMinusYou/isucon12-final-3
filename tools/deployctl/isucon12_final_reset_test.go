package main

import "testing"

func TestISUCON12FinalResetCoversEveryLocalDatabase(t *testing.T) {
	cfg, err := loadConfig(contestOverlayConfig)
	if err != nil {
		t.Fatal(err)
	}
	runner := deployRunner{
		roles: map[string][]string{
			"app":   {"isucon-1", "isucon-2", "isucon-3", "isucon-4", "isucon-5"},
			"mysql": {"isucon-1", "isucon-2", "isucon-3", "isucon-4", "isucon-5"},
		},
		vars: map[string]string{
			"remote_home": "/home/isucon", "sql_dir": "webapp/sql",
			"sql_schema_file": "setup/1_schema.sql", "db_name": "isucon",
			"listen_port": "8080", "initialize_path": "/initialize",
		},
	}
	for _, name := range []string{"db-schema", "app-initialize"} {
		jobs, err := runner.activationJobs(cfg.Deployments[name].Activations)
		if err != nil || len(jobs) != 5 {
			t.Fatalf("%s: %d jobs, %v", name, len(jobs), err)
		}
	}
}
