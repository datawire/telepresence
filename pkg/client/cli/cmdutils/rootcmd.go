package cmdutils

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

type rootCmdName struct{}

func WithRootCmdName(cmd *cobra.Command) {
	root := GetRootCmdFunc(cmd)
	name := root.Name()
	for {
		p := root.Parent()
		if p == nil {
			break
		}
		root = p
		name = fmt.Sprintf("%s %s", root.Name(), name)
	}

	ctx := cmd.Context()
	ctx = context.WithValue(ctx, rootCmdName{}, name)
	cmd.SetContext(ctx)
}

func GetRootCmdName(ctx context.Context) string {
	name, ok := ctx.Value(rootCmdName{}).(string)
	if !ok {
		// sane default
		return "telepresence"
	}
	return name
}

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
