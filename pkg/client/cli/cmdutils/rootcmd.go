package cmdutils

import "github.com/spf13/cobra"

var GetRootCmdFunc = func(cmd *cobra.Command) *cobra.Command { //nolint:gochecknoglobals // extension point
	rootCmd := cmd
	for {
		p := rootCmd.Parent()
		if p == nil {
			break
		}
		rootCmd = p
	}
	return rootCmd
}
