// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package check

import (
	"fmt"
	"os"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// injectProfileArgs adds admin API connection flags derived from the current
// rpk profile to pluginArgs, unless the user already passed them explicitly.
// This lets 'rpk check' work against remote clusters by inheriting admin_api
// and kafka_api settings from the active profile (same as other rpk commands).
func injectProfileArgs(p *config.Params, fs afero.Fs, pluginArgs []string) []string {
	cfg, err := p.Load(fs)
	if err != nil {
		zap.L().Debug("unable to load rpk config; skipping profile arg injection", zap.Error(err))
		return pluginArgs
	}
	profile := cfg.VirtualProfile()
	a := &profile.AdminAPI
	if len(a.Addresses) > 0 && !hasFlag(pluginArgs, "--admin-url") {
		pluginArgs = append(pluginArgs, "--admin-url", strings.Join(a.Addresses, ","))
	}
	if tls := a.TLS; tls != nil {
		if tls.TruststoreFile != "" && !hasFlag(pluginArgs, "--admin-tls-ca") {
			pluginArgs = append(pluginArgs, "--admin-tls-ca", tls.TruststoreFile)
		}
		if tls.CertFile != "" && !hasFlag(pluginArgs, "--admin-tls-cert") {
			pluginArgs = append(pluginArgs, "--admin-tls-cert", tls.CertFile)
		}
		if tls.KeyFile != "" && !hasFlag(pluginArgs, "--admin-tls-key") {
			pluginArgs = append(pluginArgs, "--admin-tls-key", tls.KeyFile)
		}
		if tls.InsecureSkipVerify && !hasFlag(pluginArgs, "--admin-tls-skip-verify") {
			pluginArgs = append(pluginArgs, "--admin-tls-skip-verify")
		}
	}
	if profile.KafkaAPI.SASL != nil {
		sasl := profile.KafkaAPI.SASL
		if sasl.User != "" && !hasFlag(pluginArgs, "--sasl-user") {
			pluginArgs = append(pluginArgs, "--sasl-user", sasl.User)
		}
		if sasl.Password != "" && !hasFlag(pluginArgs, "--sasl-password") {
			pluginArgs = append(pluginArgs, "--sasl-password", sasl.Password)
		}
	}
	return pluginArgs
}

func NewCommand(fs afero.Fs, p *config.Params, execFn func(string, []string) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:                "check",
		Short:              "Run production readiness checks for a Redpanda deployment",
		DisableFlagParsing: true,
		Args:               cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			pluginArgs, err := parseCheckFlags(p, cmd, args)
			out.MaybeDie(err, "unable to parse flags: %v", err)

			// Inject admin API connection info from the rpk profile so that
			// 'rpk check' works against remote clusters without the user
			// having to repeat --admin-url on every invocation.
			pluginArgs = injectProfileArgs(p, fs, pluginArgs)

			check, pluginExists := plugin.ListPlugins(fs, plugin.UserPaths()).Find("check")
			var pluginPath string
			if !pluginExists {
				var isSubcommand bool
				for _, arg := range pluginArgs {
					switch {
					case arg == "--version":
						fmt.Println("cannot get check version: redpanda-check is not installed; run 'rpk check install'")
						cmd.Help()
						return
					case strings.HasPrefix(arg, "--") || strings.HasPrefix(arg, "-"):
						continue
					default:
						isSubcommand = true
					}
				}
				if !isSubcommand {
					cmd.Help()
					return
				}
				fmt.Fprintln(os.Stderr, "Downloading latest Redpanda Check")
				path, _, err := installCheck(cmd.Context(), fs, "latest")
				out.MaybeDie(err, "unable to install redpanda check: %v; you may install 'redpanda-check' manually", err)
				pluginPath = path
			}
			if pluginExists {
				pluginPath = check.Path
				if !check.Managed {
					zap.L().Sugar().Warn("rpk is using a self-managed version of Redpanda Check. If you want rpk to manage check, use rpk check uninstall && rpk check install.")
				}
			}
			if cmd.Flags().Changed("help") {
				cmd.Help()
				return
			}
			zap.L().Debug("executing check plugin", zap.String("path", pluginPath), zap.Strings("args", pluginArgs))
			err = execFn(pluginPath, pluginArgs)
			out.MaybeDie(err, "unable to execute redpanda check plugin: %v", err)
		},
	}
	cmd.AddCommand(
		installCommand(fs),
		uninstallCommand(fs),
		upgradeCommand(fs),
	)
	return cmd
}

func parseCheckFlags(p *config.Params, cmd *cobra.Command, args []string) ([]string, error) {
	f := cmd.Flags()
	keepForPlugin, stripForRpk := cobraext.StripFlagset(args, f)
	if err := f.Parse(stripForRpk); err != nil {
		return nil, err
	}
	zap.ReplaceGlobals(p.BuildLogger())
	if cobraext.LongFlagValue(args, f, "help", "h") == "true" && !contains(keepForPlugin, "--help") {
		keepForPlugin = append(keepForPlugin, "--help")
	}
	return keepForPlugin, nil
}

func hasFlag(args []string, flag string) bool {
	for _, a := range args {
		if a == flag || strings.HasPrefix(a, flag+"=") {
			return true
		}
	}
	return false
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
