package polymarket

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func runGit(repoRoot string, args ...string) error {
	fmt.Printf("+ git %s\n", strings.Join(args, " "))
	cmd := exec.Command("git", args...)
	cmd.Dir = repoRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func runGitQuiet(repoRoot string, args ...string) (int, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = repoRoot
	var stderr bytes.Buffer
	cmd.Stdout = os.Stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		return 0, nil
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		if stderr.Len() > 0 {
			fmt.Fprint(os.Stderr, stderr.String())
		}
		return exitErr.ExitCode(), nil
	}
	return -1, err
}

func ensureGitIdentity(repoRoot string) error {
	if err := runGit(repoRoot, "config", "user.name", "github-actions[bot]"); err != nil {
		return err
	}
	if err := runGit(repoRoot, "config", "user.email", "github-actions[bot]@users.noreply.github.com"); err != nil {
		return err
	}
	return nil
}

func checkoutLatest(repoRoot string, branch string) error {
	if err := runGit(repoRoot, "fetch", "origin", branch); err != nil {
		return err
	}
	if err := runGit(repoRoot, "checkout", "-B", branch, fmt.Sprintf("origin/%s", branch)); err != nil {
		return err
	}
	if err := runGit(repoRoot, "reset", "--hard", fmt.Sprintf("origin/%s", branch)); err != nil {
		return err
	}
	return nil
}

func stageStats(repoRoot string) (bool, error) {
	rel := filepath.ToSlash(filepath.Join("reports", "polymarket_stats"))
	if err := runGit(repoRoot, "add", "-A", rel); err != nil {
		return false, err
	}
	exitCode, err := runGitQuiet(repoRoot, "diff", "--cached", "--quiet")
	if err != nil {
		return false, err
	}
	return exitCode != 0, nil
}

func commitStats(repoRoot, message string) error {
	return runGit(repoRoot, "commit", "-m", message)
}

func push(repoRoot, branch string) (bool, error) {
	cmd := exec.Command("git", "push", "origin", fmt.Sprintf("HEAD:%s", branch))
	cmd.Dir = repoRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err == nil {
		return true, nil
	}
	if _, ok := err.(*exec.ExitError); ok {
		return false, nil
	}
	return false, err
}

func RunSyncAndPush() error {
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	repoRoot := cfg.RepoRoot
	branch := cfg.DefaultBranch
	commitMessage := cfg.StatsCommitMessage
	maxAttempts := cfg.StatsPushMaxAttempts

	if err := ensureGitIdentity(repoRoot); err != nil {
		return err
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		fmt.Printf("[STATS] attempt %d/%d\n", attempt, maxAttempts)
		if err := checkoutLatest(repoRoot, branch); err != nil {
			return err
		}
		if err := os.Chdir(repoRoot); err != nil {
			return err
		}
		if err := RunStatsReport(); err != nil {
			return err
		}
		changed, err := stageStats(repoRoot)
		if err != nil {
			return err
		}
		if !changed {
			fmt.Println("[STATS] No report changes detected. Nothing to push.")
			return nil
		}
		if err := commitStats(repoRoot, commitMessage); err != nil {
			return err
		}
		ok, err := push(repoRoot, branch)
		if err != nil {
			return err
		}
		if ok {
			fmt.Println("[STATS] Push succeeded.")
			return nil
		}
		fmt.Println("[STATS] Push rejected. Retrying from latest remote state...")
	}
	return fmt.Errorf("[STATS] failed to push stats after retries")
}
