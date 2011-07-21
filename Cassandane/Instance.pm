#!/usr/bin/perl
#
#  Copyright (c) 2011 Opera Software Australia Pty. Ltd.  All rights
#  reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions
#  are met:
#
#  1. Redistributions of source code must retain the above copyright
#     notice, this list of conditions and the following disclaimer.
#
#  2. Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions and the following disclaimer in
#     the documentation and/or other materials provided with the
#     distribution.
#
#  3. The name "Opera Software Australia" must not be used to
#     endorse or promote products derived from this software without
#     prior written permission. For permission or any legal
#     details, please contact
# 	Opera Software Australia Pty. Ltd.
# 	Level 50, 120 Collins St
# 	Melbourne 3000
# 	Victoria
# 	Australia
#
#  4. Redistributions of any form whatsoever must retain the following
#     acknowledgment:
#     "This product includes software developed by Opera Software
#     Australia Pty. Ltd."
#
#  OPERA SOFTWARE AUSTRALIA DISCLAIMS ALL WARRANTIES WITH REGARD TO
#  THIS SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
#  AND FITNESS, IN NO EVENT SHALL OPERA SOFTWARE AUSTRALIA BE LIABLE
#  FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
#  WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
#  AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING
#  OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#

package Cassandane::Instance;
use strict;
use warnings;
use File::Path qw(mkpath rmtree);
use File::Find qw(find);
use POSIX qw(geteuid :signal_h :sys_wait_h);
use Time::HiRes qw(sleep gettimeofday tv_interval);
use DateTime;
use BSD::Resource;
use Cwd qw(abs_path);
use Cassandane::Util::DateTime qw(to_iso8601);
use Cassandane::Util::Log;
use Cassandane::Config;
use Cassandane::Service;
use Cassandane::ServiceFactory;

my $rootdir = '/var/tmp/cass';
my $stamp;
my $next_unique = 1;
my %defaults =
(
    valgrind => 0,
);

sub new
{
    my $class = shift;
    my %params = @_;
    my $self = {
	name => undef,
	basedir => undef,
	cyrus_prefix => '/usr/cyrus',
	config => Cassandane::Config->default()->clone(),
	services => {},
	re_use_dir => 0,
	setup_mailbox => 1,
	persistent => 0,
	valgrind => $defaults{valgrind},
	_children => {},
	_stopped => 0,
    };

    $self->{name} = $params{name}
	if defined $params{name};
    $self->{basedir} = $params{basedir}
	if defined $params{basedir};
    $self->{cyrus_prefix} = $params{cyrus_prefix}
	if defined $params{cyrus_prefix};
    $self->{config} = $params{config}->clone()
	if defined $params{config};
    $self->{re_use_dir} = $params{re_use_dir}
	if defined $params{re_use_dir};
    $self->{setup_mailbox} = $params{setup_mailbox}
	if defined $params{setup_mailbox};
    $self->{valgrind} = $params{valgrind}
	if defined $params{valgrind};
    $self->{persistent} = $params{persistent}
	if defined $params{persistent};

    if (!defined $stamp)
    {
	$stamp = to_iso8601(DateTime->now);
	$stamp =~ s/.*T(\d+)Z/$1/;
    }

    if (!defined $self->{name})
    {
	for (;;)
	{
	    $self->{name} = $stamp . $next_unique;
	    $next_unique++;
	    last unless -d "$rootdir/$self->{name}";
	}
    }
    $self->{basedir} = $rootdir . '/' . $self->{name}
	unless defined $self->{basedir};
    $self->{config}->set_variables(
		name => $self->{name},
		basedir => $self->{basedir},
	    );

    bless $self, $class;
    xlog "basedir $self->{basedir}";
    return $self;
}

sub set_defaults
{
    my ($class, %params) = @_;

    foreach my $p (qw(valgrind))
    {
	$defaults{$p} = $params{$p}
	    if defined $params{$p};
    }
}

sub add_service
{
    my ($self, $name, %params) = @_;

    die "Already have a service named \"$name\""
	if defined $self->{services}->{$name};

    my $srv = Cassandane::ServiceFactory->create($name, %params);
    $self->{services}->{$name} = $srv;
    return $srv;
}

sub get_service
{
    my ($self, $name) = @_;
    return $self->{services}->{$name};
}

sub _binary
{
    my ($self, $name) = @_;

    my @cmd;

    if ($self->{valgrind})
    {
	my $valgrind_logdir = $self->{basedir} . '/vglogs';
	my $valgrind_suppressions = abs_path('vg.supp');
	mkpath $valgrind_logdir
	    unless ( -d $valgrind_logdir );
	push(@cmd,
	    '/usr/bin/valgrind',
	    '-q',
	    "--log-file=$valgrind_logdir/$name.%p",
	    "--suppressions=$valgrind_suppressions",
	    '--tool=memcheck',
	    '--leak-check=full'
	);
    }

    my $bin = $name;
    $bin = $self->{cyrus_prefix} . '/bin/' . $bin
	unless $bin =~ m/^\//;
    push(@cmd, $bin);

    return @cmd;
}

sub _imapd_conf
{
    my ($self) = @_;

    return $self->{basedir} . '/conf/imapd.conf';
}

sub _master_conf
{
    my ($self) = @_;

    return $self->{basedir} . '/conf/cyrus.conf';
}

sub _pid_file
{
    my ($self) = @_;

    return $self->{basedir} . '/run/cyrus.pid';
}

sub _build_skeleton
{
    my ($self) = @_;

    my @subdirs =
    (
	'conf',
	'conf/cores',
	'conf/db',
	'conf/sieve',
	'conf/socket',
	'conf/proc',
	'conf/log',
	'conf/log/admin',
	'conf/log/cassandane',
	'lock',
	'data',
	'meta',
	'run',
    );
    foreach my $sd (@subdirs)
    {
	my $d = $self->{basedir} . '/' . $sd;
	mkpath $d
	    or die "Cannot make path $d: $!";
    }
}

sub _generate_imapd_conf
{
    my ($self) = @_;

    $self->{config}->generate($self->_imapd_conf());
}

sub _generate_master_conf
{
    my ($self) = @_;

    my $filename = $self->_master_conf();
    open MASTER,'>',$filename
	or die "Cannot open $filename for writing: $!";
    print MASTER "SERVICES {\n";

    foreach my $srv (values %{$self->{services}})
    {
	my $mp = $srv->master_params();

	# Fix up {cmd}
	my $bin = shift @{$mp->{cmd}};
	$mp->{cmd} = join(' ',
	    $self->_binary($bin),
	    '-C', $self->_imapd_conf(),
	    @{$mp->{cmd}}
	);

	print MASTER "    $srv->{name}";
	while (my ($k, $v) = each %$mp)
	{
	    $v = "\"$v\""
		if ($v =~ m/\s/);
	    print MASTER " $k=$v";
	}
	print MASTER "\n";
    }

    print MASTER "}\n";
    close MASTER;
}

sub _fix_ownership
{
    my ($self, $path) = @_;

    $path ||= $self->{basedir};

    return if geteuid() != 0;
    my $uid = getpwnam('cyrus');
    my $gid = getgrnam('root');

    find(sub { chown($uid, $gid, $File::Find::name) }, $path);
}

sub _timed_wait
{
    my ($condition, %p) = @_;
    $p{delay} = 0.010		# 10 millisec
	unless defined $p{delay};
    $p{maxwait} = 20.0
	unless defined $p{maxwait};
    $p{description} = 'unknown condition'
	unless defined $p{description};

    my $start = [gettimeofday()];
    my $delayed = 0;
    while ( ! $condition->() )
    {
	die "Timed out waiting for " . $p{description}
	    if (tv_interval($start, [gettimeofday()]) > $p{maxwait});
	sleep($p{delay});
	$delayed = 1;
	$p{delay} *= 1.5;	# backoff
    }

    xlog "_timed_wait: waited " .
	tv_interval($start, [gettimeofday()]) .
	" sec for " .
	$p{description}
	if ($delayed);
}

sub _read_pid_file
{
    my ($self) = @_;
    my $file = $self->_pid_file();
    my $pid;

    return undef if ( ! -f $file );

    open PID,'<',$file
	or return undef;
    while(<PID>)
    {
	chomp;
	($pid) = m/^(\d+)$/;
	last;
    }
    close PID;

    return undef unless defined $pid;
    return undef unless $pid > 1;
    return undef unless kill(0, $pid) > 0;
    return $pid;
}

sub _start_master
{
    my ($self) = @_;

    # First check that nothing is listening on any of the ports
    # we expect to be able to use.  That would indicate a failure
    # of test containment - i.e. we failed to shut something down
    # earlier.  Or it might indicate that someone is trying to run
    # a second set of Cassandane tests on this machine, which is
    # also going to fail miserably.  In any case we want to know.
    foreach my $srv (values %{$self->{services}})
    {
	die "Some process is already listening on " . $srv->address()
	    if $srv->is_listening();
    }

    # Now start the master process.
    my @cmd =
    (
	'master',
	# The following is added automatically by _fork_utility:
	# '-C', $self->_imapd_conf(),
	'-l', '255',
	'-p', $self->_pid_file(),
	'-d',
	'-M', $self->_master_conf(),
    );
    unlink $self->_pid_file();
    # _fork_utility() returns a pid, but that doesn't help
    # because master will fork again to background itself.
    $self->_fork_utility(@cmd);

    # wait until the pidfile exists and contains a PID
    # that we can verify is still alive.
    xlog "_start_master: waiting for PID file";
    _timed_wait(sub { $self->_read_pid_file() },
	        description => "the master PID file to exist");
    xlog "_start_master: PID file present and correct";

    # Wait until all the defined services are reported as listening.
    # That doesn't mean they're ready to use but it means that at least
    # a client will be able to connect(), although the first response
    # might be a bit slow.
    xlog "_start_master: PID waiting for services";
    foreach my $srv (values %{$self->{services}})
    {
	next unless $srv->{port} =~ m/^\d+$/;
	_timed_wait(sub { $srv->is_listening() },
	        description => $srv->address() . " to be in LISTEN state");
    }
    xlog "_start_master: all services listening";
}

sub start
{
    my ($self) = @_;

    my $created = 0;

    xlog "start";
    if (!$self->{re_use_dir} || ! -d $self->{basedir})
    {
	$created = 1;
	rmtree $self->{basedir};
	$self->_build_skeleton();
	# TODO: system("echo 1 >/proc/sys/kernel/core_uses_pid");
	# TODO: system("echo 1 >/proc/sys/fs/suid_dumpable");
	$self->_generate_imapd_conf();
	$self->_generate_master_conf();
	$self->_fix_ownership();
    }
    $self->_start_master();

    if ($created && $self->{setup_mailbox} && defined $self->get_service('imap'))
    {
	my $owner = "cassandane";

	xlog "create user $owner";
	my $adminstore = $self->get_service('imap')->create_store(username => 'admin');
	my $adminclient = $adminstore->get_client();
	$adminclient->create("user.$owner");
	$adminclient->setacl("user.$owner", admin => 'lrswipkxtecda');
	$adminclient->setacl("user.$owner", $owner => 'lrswipkxtecd');
	$adminclient->setacl("user.$owner", anyone => 'p');
    }
}

sub _stop_pid
{
    my ($pid) = @_;

    # try to be nice
    xlog "_stop_pid: sending SIGQUIT to $pid";
    kill(SIGQUIT, $pid);
    eval {
	_timed_wait(sub { kill(0, $pid) == 0 });
    };
    if ($@)
    {
	# Timed out -- No More Mr Nice Guy
	xlog "_stop_pid: sending SIGTERM to $pid";
	kill(SIGTERM, $pid);
    }
}

sub stop
{
    my ($self) = @_;

    return if ($self->{persistent});
    return if ($self->{_stopped});
    $self->{_stopped} = 1;

    xlog "stop";

    my $pid = $self->_read_pid_file();
    _stop_pid($pid) if defined $pid;

#     rmtree $self->{basedir};
}

sub DESTROY
{
    my ($self) = @_;

    if (!$self->{persistent} && !$self->{_stopped})
    {
	my $pid = $self->_read_pid_file();
	if (defined $pid)
	{
	    # clean up any dangling master process
	    xlog "cleaning up $pid";
	    kill(SIGKILL, $pid);
	}
    }
}

sub deliver
{
    my ($self, $msg) = @_;
    my $str = $msg->as_string();
    my $fh = $self->run_utility('|-', 'deliver', 'cassandane');
    print $fh $str;
    close($fh);
}

# Run a Cyrus utility program with the given arguments.  The first
# argument may optionally be a mode string, either '-|' or '|-' which
# affects how the utility's stdin and stdout are treated thus:
#
# -|	    stdin is redirected from /dev/null,
#	    stdout is captured in the returned file handle,
#	    stderr is redirected to /dev/null (or is unmolested
#		is xlog is in verbose mode).
#	    returns a new file handle from the running process
#
# |-	    stdin is fed from the returned file handle
#	    stdout is redirected to /dev/null (or is unmolested
#		is xlog is in verbose mode).
#	    stderr likewise
#	    returns a new file handle to the running process
#
# (none)    stdin is redirected from /dev/null,
#	    stdout is redirected to /dev/null (or is unmolested
#		is xlog is in verbose mode).
#	    stderr likewise
#	    returns UNIX exit code to the finished process
#	    and dies if the process failed
#
sub run_utility
{
    my ($self, @args) = @_;

    my ($pid, $fh) = $self->_fork_utility(@args);

    return $fh
	if defined $fh;

    # parent process...wait for child
    my $child = waitpid($pid,0);
    # and deal with it's exit status
    return $self->_handle_wait_status($pid)
	if $child == $pid;
    return undef;
}

#
# Starts a new process to run a Cyrus utility program.  Obeys
# the "mode" argument like run_utility().
#
# Returns: ($pid, $fh, $desc) where $fh is the file handle of the
#	   pipe to the captured input or output, or undef if
#	   no capturing.  close()ing the filehandle does a
#	   waitpid(); you must call _handle_wait_status() to
#	   decode $?.  Dies on errors.
#
sub _fork_utility
{
    my ($self, $mode, @argv) = @_;
    my $binary;

    die "No mode or binary specified"
	unless defined $mode;

    my %redirects;
    if ($mode eq '-|')
    {
	# stdin is null, capture stdout
	$redirects{stdin} = '/dev/null';
	$binary = shift @argv;
    }
    elsif ($mode eq '|-')
    {
	# feed stdin, stdout is null or unmolested
	$redirects{stdout} = '/dev/null'
	    unless get_verbose;
	$binary = shift @argv;
    }
    elsif ($mode eq 'gdb')
    {
	# stdin, stdout unmolested
	$binary = shift @argv;
    }
    else
    {
	# stdin is null, stdout is null or unmolested
	$redirects{stdin} = '/dev/null';
	$redirects{stdout} = '/dev/null'
	    unless get_verbose;
	$binary = $mode;
	$mode = undef;
    }
    $redirects{stderr} = '/dev/null'
	unless get_verbose;
    die "No binary specified"
	unless defined $binary;

    my @cmd =
    (
	$self->_binary($binary),
	'-C', $self->_imapd_conf(),
	@argv,
    );

    if (defined $mode && $mode eq 'gdb')
    {
	my $gdbx = '/var/tmp/gdb.x';
	open GDBX, '>', $gdbx
	    or die "Cannot open $gdbx for writing: $!";
	print GDBX "file " . shift(@cmd) . "\n";
	print GDBX "set args " . join(' ', @cmd) . "\n";
	close GDBX;
	@cmd = ( 'gdb', '-x', $gdbx );
	$mode = undef;
    }

    xlog "Running: " . join(' ', map { "\"$_\"" } @cmd);

    if (defined $mode)
    {
	my $fh;
	# Use the fork()ing form of open()
	my $pid = open $fh,$mode;
	die "Cannot fork: $!"
	    if !defined $pid;
	if ($pid)
	{
	    # parent process
	    $self->{_children}->{$fh} = "(binary $binary pid $pid)";
	    return ($pid, $fh);
	}
    }
    else
    {
	# No capturing - just plain fork()
	my $pid = fork();
	die "Cannot fork: $!"
	    if !defined $pid;
	if ($pid)
	{
	    # parent process
	    $self->{_children}->{$pid} = "(binary $binary pid $pid)";
	    return ($pid, undef);
	}
	return ($pid, undef, "$binary (pid $pid)")
	    if ($pid);	    # parent process
    }

    # child process

    my $cd = $self->{basedir} . '/conf/cores';
    chdir($cd)
	or die "Cannot cd to $cd: $!";

    # ulimit -c 102400
    setrlimit(RLIMIT_CORE, 102400*1024, 102400*1024);

    # TODO: do any setuid, umask, or environment futzing here

    # implement redirects
    if (defined $redirects{stdin})
    {
	open STDIN,'<',$redirects{stdin}
	    or die "Cannot redirect STDIN from $redirects{stdin}: $!";
    }
    if (defined $redirects{stdout})
    {
	open STDOUT,'>',$redirects{stdout}
	    or die "Cannot redirect STDOUT to $redirects{stdout}: $!";
    }
    if (defined $redirects{stderr})
    {
	open STDERR,'>',$redirects{stderr}
	    or die "Cannot redirect STDERR to $redirects{stderr}: $!";
    }

    exec @cmd;
    die "Cannot run $binary: $!";
}

sub _handle_wait_status
{
    my ($self, $key) = @_;
    my $status = $?;

    my $desc = $self->{_children}->{$key} || "unknown";
    delete $self->{_children}->{$key};

    if (WIFSIGNALED($status))
    {
	my $sig = WTERMSIG($status);
	die "child process $desc terminated by signal $sig";
    }
    elsif (WIFEXITED($status))
    {
	my $code = WEXITSTATUS($status);
	die "child process $desc exited with code $code"
	    if $code != 0;
    }
    else
    {
	die "WTF? Cannot decode wait status $status";
    }
    return 0;
}

sub describe
{
    my ($self) = @_;

    print "Cyrus instance\n";
    printf "    name: %s\n", $self->{name};
    printf "    imapd.conf: %s\n", $self->_imapd_conf();
    printf "    services:\n";
    foreach my $srv (values %{$self->{services}})
    {
	printf "        ";
	$srv->describe();
    }
}

sub start_replicated_pair
{
    my ($class, $conf, %params) = @_;

    my $port = Cassandane::Service->alloc_port();
    $conf ||= Cassandane::Config->default();
    $conf = $conf->clone();
    $conf->set(
	# sync_client will find the port in the config
	sync_port => $port,
	# tell sync_client how to login
	sync_authname => 'repluser',
	sync_password => 'replpass',
	sync_realm => 'internal',
	sasl_mech_list => 'PLAIN',
	# Ensure sync_server gives sync_client enough privileges
	admins => 'admin repluser',
    );

    my $master = Cassandane::Instance->new(config => $conf);

    my $replica = Cassandane::Instance->new(config => $conf,
					    setup_mailbox => 0);
    $replica->add_service('sync', port => $port);

    my $services = $params{'services'} || [ 'imap' ];
    my $has_imap = 0;
    foreach my $s (@$services)
    {
	$master->add_service($s);
	$replica->add_service($s);
	$has_imap = 1 if ($s eq 'imap');
    }

    $master->start();
    $replica->start();

    # Run the replication engine to create the user mailbox
    # in the replica.  Doing it this way avoids issues with
    # mismatched mailbox uniqueids.
    $class->run_replication($master, $replica);

    # Use INBOX because we know it exists at both ends.
    my %store_params = ( folder => 'INBOX' );
    my $master_store;
    $master_store = $master->get_service('imap')->create_store(%store_params)
	if $has_imap;

    my $replica_store;
    $replica_store = $replica->get_service('imap')->create_store(%store_params)
	if $has_imap;

    return ($master, $replica, $master_store, $replica_store);
}

sub run_replication
{
    my ($class, $master, $replica, $master_store, $replica_store) = @_;

    xlog "running replication";

    # Disconnect during replication to ensure no imapd
    # is locking the mailbox, which gives us a spurious
    # error which is ignored in real world scenarios.
    $master_store->disconnect() if defined $master_store;
    $replica_store->disconnect() if defined $replica_store;

    my $params =
	$replica->get_service('sync')->store_params();

    # TODO: need a timeout!!

    $master->run_utility('sync_client',
	'-v',			# verbose
	'-v',			# even more verbose
	'-S', $params->{host},	# hostname to connect to
	'-u', 'cassandane',	# replicate the Cassandane user
	);


    if (defined $master_store)
    {
	$master_store->_connect();
	$master_store->_select();
    }
    if (defined $replica_store)
    {
	$replica_store->_connect();
	$replica_store->_select();
    }
}


1;
