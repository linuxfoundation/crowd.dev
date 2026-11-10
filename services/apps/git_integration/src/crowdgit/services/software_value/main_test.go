package main

import (
	"os/exec"
	"testing"
)

func TestGetGitRepositoryURL(t *testing.T) {
	cases := map[string]string{
		"https://github.com/NVIDIA/OpenShell.git":  "https://github.com/NVIDIA/OpenShell",
		"https://github.com/NVIDIA/OpenShell":      "https://github.com/NVIDIA/OpenShell",
		"http://gitlab.com/group/project.git":      "https://gitlab.com/group/project",
		"git@github.com:kubernetes/kubernetes.git": "https://github.com/kubernetes/kubernetes",
		"ssh://git@github.com/cncf/landscape.git":  "https://github.com/cncf/landscape",
	}

	for remote, want := range cases {
		dir := t.TempDir()
		for _, args := range [][]string{{"init", "-q"}, {"remote", "add", "origin", remote}} {
			cmd := exec.Command("git", args...)
			cmd.Dir = dir
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("git %v: %v: %s", args, err, out)
			}
		}

		got, err := getGitRepositoryURL(dir)
		if err != nil {
			t.Fatalf("%s: %v", remote, err)
		}
		if got != want {
			t.Errorf("%s: got %q, want %q", remote, got, want)
		}
	}
}
