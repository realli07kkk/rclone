// Package copy provides the copy command.
package copy

import (
	"strings"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/fs/config/flags"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/operations/operationsflags"
	"github.com/rclone/rclone/fs/sync"
	"github.com/spf13/cobra"
)

var (
	createEmptySrcDirs = false
	gracefulShutdown   = false
	loggerOpt          = operations.LoggerOpt{}
	loggerFlagsOpt     = operationsflags.AddLoggerFlagsOptions{}
)

func init() {
	cmd.Root.AddCommand(commandDefinition)
	cmdFlags := commandDefinition.Flags()
	flags.BoolVarP(cmdFlags, &createEmptySrcDirs, "create-empty-src-dirs", "", createEmptySrcDirs, "Create empty source dirs on destination after copy", "")
	flags.BoolVarP(cmdFlags, &gracefulShutdown, "graceful-shutdown", "", gracefulShutdown, "On SIGTERM, stop starting objects and let active transfers finish", "")
	operationsflags.AddLoggerFlags(cmdFlags, &loggerOpt, &loggerFlagsOpt)
	loggerOpt.LoggerFn = operations.NewDefaultLoggerFn(&loggerOpt)
}

var commandDefinition = &cobra.Command{
	Use:   "copy source:path dest:path",
	Short: `Copy files from source to dest, skipping identical files.`,
	// Note: "|" will be replaced by backticks below
	Long: strings.ReplaceAll(`Copy the source to the destination.  Does not transfer files that are
identical on source and destination, testing by size and modification
time or MD5SUM.  Doesn't delete files from the destination. If you
want to also delete files from destination, to make it match source,
use the [sync](/commands/rclone_sync/) command instead.

Note that it is always the contents of the directory that is synced,
not the directory itself. So when source:path is a directory, it's the
contents of source:path that are copied, not the directory name and
contents.

To copy single files, use the [copyto](/commands/rclone_copyto/)
command instead.

If dest:path doesn't exist, it is created and the source:path contents
go there.

For example

|||sh
rclone copy source:sourcepath dest:destpath
|||

Let's say there are two files in sourcepath

|||text
sourcepath/one.txt
sourcepath/two.txt
|||

This copies them to

|||text
destpath/one.txt
destpath/two.txt
|||

Not to

|||text
destpath/sourcepath/one.txt
destpath/sourcepath/two.txt
|||

If you are familiar with |rsync|, rclone always works as if you had
written a trailing |/| - meaning "copy the contents of this directory".
This applies to all commands and whether you are talking about the
source or destination.

See the [--no-traverse](/docs/#no-traverse) option for controlling
whether rclone lists the destination directory or not.  Supplying this
option when copying a small number of files into a large destination
can speed transfers up greatly.

For example, if you have many files in /path/to/src but only a few of
them change every day, you can copy all the files which have changed
recently very efficiently like this:

|||sh
rclone copy --max-age 24h --no-traverse /path/to/src remote:
|||

Rclone will sync the modification times of files and directories if
the backend supports it. If metadata syncing is required then use the
|--metadata| flag.

Note that the modification time and metadata for the root directory
will **not** be synced. See [issue #7652](https://github.com/rclone/rclone/issues/7652)
for more info.

**Note**: Use the |-P|/|--progress| flag to view real-time transfer statistics.

**Note**: Use the |--dry-run| or the |--interactive|/|-i| flag to test without
copying anything.

With |--graceful-shutdown|, the first SIGTERM stops scanning, checking and
starting new objects. Queued objects are skipped. Active objects finish their
transfers, verification and cleanup, then rclone prints statistics, closes
reports and backends, and exits with code 143. A second SIGTERM uses the usual
exit cleanup without waiting for transfers; Ctrl+C retains its usual behavior.
This option is disabled by default and is available only for |copy|, including
when its source is a single file.

There is no shutdown grace deadline. Existing connection, idle I/O, backend
and parent context timeouts still apply. |--transfer-timeout| retains each
object's original deadline; |0| or |off| leaves it disabled. |--max-duration|
retains the current pass's deadline: HARD cancels active transfers, while
other cutoff modes stop starting new ones. Low-level retries, bandwidth and
transfer limits remain in effect; high-level retries stop, including retry
waits. Real failures remain in statistics and error reports, but an accepted
SIGTERM always gives exit code 143.

In particular, |--transfer-timeout 0 --timeout 30s| does not guarantee exit
within 30 seconds of SIGTERM: a continuously flowing transfer can run until
completion. Cleanup can take additional time; with an object timeout enabled,
remote cleanup shares the existing budget of up to 30 seconds. Cancellation
responsiveness depends on the backend.

`, "|", "`") + operationsflags.Help(),
	Annotations: map[string]string{
		"groups": "Copy,Filter,Listing,Important",
	},
	Run: func(command *cobra.Command, args []string) {
		cmd.CheckArgs(2, 2, command, args)
		if gracefulShutdown {
			defer cmd.EnableGracefulShutdown(command)()
		}
		fsrc, srcFileName, fdst := cmd.NewFsSrcFileDst(args)
		cmd.Run(true, true, command, func() error {
			ctx := command.Context()
			close, err := operationsflags.ConfigureLoggers(ctx, fdst, command, &loggerOpt, loggerFlagsOpt)
			if err != nil {
				return err
			}
			defer close()

			if loggerFlagsOpt.AnySet() {
				ctx = operations.WithSyncLogger(ctx, loggerOpt)
			}

			if srcFileName == "" {
				return sync.CopyDir(ctx, fdst, fsrc, createEmptySrcDirs)
			}
			return operations.CopyFile(ctx, fdst, fsrc, srcFileName, srcFileName)
		})
	},
}
