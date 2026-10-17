package main

import (
	"fmt"
	"slices"
	"sort"
)

const (
	phaseLadder  = "ladder"
	phaseBisect  = "bisect"
	phaseConfirm = "confirm"
)

// stepConfig identifies one executed step-run. Step is the 1-based index of the
// (cells, rate) configuration: ladder steps use their profile index, bisect configurations
// continue after the profile steps. Run counts repeats of that configuration: 1 for the
// ladder/bisect run, 2.. for confirmation runs.
type stepConfig struct {
	Step, Run, Cells, Rate int
}

func (c stepConfig) Offered() int { return cellStreams * c.Cells * c.Rate }

// stepOutcome is what the driver needs from one executed step-run.
type stepOutcome struct {
	Verdict string
	Reasons []string
	Summary *stepSummary
	// SummaryPath is the step-summary file relative to the output directory.
	SummaryPath string
}

// stepExecutor runs one step-run; an error is a harness failure that aborts the whole run.
type stepExecutor func(stepConfig) (stepOutcome, error)

type runRecord struct {
	Phase       string   `json:"phase"`
	Step        int      `json:"step"`
	Run         int      `json:"run"`
	Cells       int      `json:"cells"`
	Rate        int      `json:"ratePerStreamPerSecond"`
	Offered     int      `json:"offeredPublishPerSecond"`
	Verdict     string   `json:"verdict"`
	Reasons     []string `json:"reasons"`
	SummaryPath string   `json:"stepSummary,omitempty"`

	summary *stepSummary
}

type adoptedStep struct {
	Step        int   `json:"step"`
	Cells       int   `json:"cells"`
	Rate        int   `json:"ratePerStreamPerSecond"`
	Offered     int   `json:"offeredPublishPerSecond"`
	ConfirmRuns []int `json:"confirmRuns"`
}

type capacityOutcome struct {
	Runs            []runRecord  `json:"runs"`
	Adopted         *adoptedStep `json:"adopted"`
	NullReason      string       `json:"nullReason,omitempty"`
	LadderExhausted bool         `json:"ladderExhausted"`
	Aborted         string       `json:"aborted,omitempty"`
}

// driveCapacity is the §3.9 search: climb the ladder until the first failing step, bisect
// the cell count between the last passing and first failing step, then confirm the highest
// passing configuration with additional independent runs, falling back to the next lower
// passing configuration when a confirmation run fails. Invalid step-runs abort the search:
// they say nothing about capacity and must be re-measured after the cause is fixed.
func driveCapacity(p Profile, execute stepExecutor) (capacityOutcome, error) {
	var out capacityOutcome
	run := func(phase string, cfg stepConfig) (string, error) {
		result, err := execute(cfg)
		if err != nil {
			return "", err
		}
		record := runRecord{
			Phase: phase, Step: cfg.Step, Run: cfg.Run, Cells: cfg.Cells, Rate: cfg.Rate, Offered: cfg.Offered(),
			Verdict: result.Verdict, Reasons: result.Reasons, SummaryPath: result.SummaryPath, summary: result.Summary,
		}
		out.Runs = append(out.Runs, record)
		if result.Verdict == verdictInvalid {
			out.Aborted = fmt.Sprintf("step %d run %d (%s) was invalid: %v", cfg.Step, cfg.Run, phase, result.Reasons)
		}
		return result.Verdict, nil
	}

	var passes []stepConfig
	var firstFail *stepConfig
	for index, step := range p.Steps {
		cfg := stepConfig{Step: index + 1, Run: 1, Cells: step.Cells, Rate: step.RatePerStreamPerSecond}
		verdict, err := run(phaseLadder, cfg)
		if err != nil {
			return out, err
		}
		if verdict == verdictInvalid {
			out.NullReason = out.Aborted
			return out, nil
		}
		if verdict == verdictPass {
			passes = append(passes, cfg)
			continue
		}
		firstFail = &cfg
		break
	}
	out.LadderExhausted = firstFail == nil

	if firstFail != nil && len(passes) > 0 {
		lastPass := passes[len(passes)-1]
		rate := firstFail.Rate
		low, high := lastPass.Cells, firstFail.Cells
		if lastPass.Rate != rate {
			low = lastPass.Offered() / (cellStreams * rate)
		}
		for round := 0; round < p.BisectRounds && high-low >= 2; round++ {
			mid := (low + high) / 2
			cfg := stepConfig{Step: len(p.Steps) + round + 1, Run: 1, Cells: mid, Rate: rate}
			verdict, err := run(phaseBisect, cfg)
			if err != nil {
				return out, err
			}
			switch verdict {
			case verdictInvalid:
				out.NullReason = out.Aborted
				return out, nil
			case verdictPass:
				low = mid
				passes = append(passes, cfg)
			default:
				high = mid
			}
		}
	}

	if len(passes) == 0 {
		out.NullReason = "the first ladder step failed; there is no passing step to confirm"
		return out, nil
	}
	candidates := slices.Clone(passes)
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Offered() > candidates[j].Offered() })
	for _, candidate := range candidates {
		confirmed := true
		var runs []int
		for repeat := 2; repeat <= 1+p.ConfirmRuns; repeat++ {
			cfg := candidate
			cfg.Run = repeat
			verdict, err := run(phaseConfirm, cfg)
			if err != nil {
				return out, err
			}
			if verdict == verdictInvalid {
				out.NullReason = out.Aborted
				return out, nil
			}
			if verdict != verdictPass {
				confirmed = false
				break
			}
			runs = append(runs, repeat)
		}
		if confirmed {
			out.Adopted = &adoptedStep{Step: candidate.Step, Cells: candidate.Cells, Rate: candidate.Rate, Offered: candidate.Offered(), ConfirmRuns: runs}
			return out, nil
		}
	}
	out.NullReason = "no passing configuration survived its confirmation runs"
	return out, nil
}
