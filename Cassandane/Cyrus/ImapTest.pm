#!/usr/bin/perl
#
#  Copyright (c) 2012 Opera Software Australia Pty. Ltd.  All rights
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

use strict;
use warnings;
package Cassandane::Cyrus::ImapTest;
use base qw(Cassandane::Cyrus::TestCase);
use Cwd qw(abs_path);
use File::Path qw(mkpath);
use DateTime;
use Cassandane::Util::Log;
use Cassandane::Cassini;

my $basedir;
my $binary;
my $testdir;
my %suppressed;

sub init
{
    my $cassini = Cassandane::Cassini->instance();
    $basedir = $cassini->val('imaptest', 'basedir');
    return unless defined $basedir;
    $basedir = abs_path($basedir);

    my $supp = $cassini->val('imaptest', 'suppress',
			     'listext subscribe');
    map { $suppressed{$_} = 1; } split(/\s+/, $supp);

    $binary = "$basedir/src/imaptest";
    $testdir = "$basedir/src/tests";
}
init;

sub new
{
    my $class = shift;
    return $class->SUPER::new({}, @_);
}

sub set_up
{
    my ($self) = @_;
    $self->SUPER::set_up();
}

sub tear_down
{
    my ($self) = @_;
    $self->SUPER::tear_down();
}

sub list_tests
{
    my @tests;

    if (!defined $basedir)
    {
	return ( 'test_warning_imaptest_is_not_installed' );
    }

    opendir TESTS, $testdir
	or die "Cannot open directory $testdir: $!";
    while (my $e = readdir TESTS)
    {
	next if $e =~ m/^\./;
	next if $e =~ m/\.mbox$/;
	next if $suppressed{$e};
	next if ( ! -f "$testdir/$e" );
	push(@tests, "test_$e");
    }
    closedir TESTS;

    return @tests;
}

sub run_test
{
    my ($self) = @_;

    if (!defined $basedir)
    {
	xlog "ImapTests are not enabled.  To enabled them, please";
	xlog "install ImapTest from http://www.imapwiki.org/ImapTest/";
	xlog "and edit [imaptest]basedir in cassandane.ini";
	xlog "This is not a failure";
	return;
    }

    my $name = $self->name();
    $name =~ s/^test_//;

    die "No such test: $name"
	if ( ! -f "$testdir/$name" );
    my $mbox = "$name.mbox";
    $mbox = "default.mbox"
	if ( ! -f "$testdir/$mbox" );

    my $farm = $self->{instance}->{basedir} . "/" . $name . ".d";
    mkpath($farm)
	or die "Cannot mkpath($farm): $!";
    symlink("$testdir/$name", "$farm/$name")
	or die "Cannot symlink $testdir/$name to $farm/$name: $!";
    symlink("$testdir/$mbox", "$farm/$mbox")
	or die "Cannot symlink $testdir/$mbox to $farm/$mbox: $!";

    my $svc = $self->{instance}->get_service('imap');
    my $params = $svc->store_params();

    my $errfile = $self->{instance}->{basedir} .  "/$name.errors";
    my $status;
    $self->{instance}->run_command({
	    redirects => { stderr => $errfile },
	    handlers => {
		exited_normally => sub { $status = 1; },
		exited_abnormally => sub { $status = 0; },
	    },
	},
	$binary,
	"host=" . $params->{host},
	"port=" . $params->{port},
	"user=" . $params->{username},
	"pass=" . $params->{password},
	"rawlog",
	"box=inbox.imaptest",
	"test=$farm");

    if ((!$status || get_verbose) && -f $errfile)
    {
	open FH, '<', $errfile
	    or die "Cannot open $errfile for reading: $!";
	while (readline FH)
	{
	    xlog $_;
	}
	close FH;
    }
    $self->assert($status);
}

1;
