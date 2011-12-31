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

use strict;
use warnings;
package Cassandane::Cyrus::Master;
use base qw(Cassandane::Cyrus::TestCase);
use POSIX qw(getcwd);
use DateTime;
use Cassandane::Util::Log;
use Cassandane::Util::Wait;
use Cassandane::Util::Socket;
use Cassandane::Instance;
use Cassandane::Service;
use Cassandane::Config;

my $lemming_bin = getcwd() . '/utils/lemming';

die "No lemming binary.  Did you run \"make\" in the Cassandane directory?"
    unless -f $lemming_bin;

sub new
{
    my $class = shift;
    my $self = $class->SUPER::new({ instance => 0 }, @_);

    return $self;
}

sub set_up
{
    my ($self) = @_;
    $self->SUPER::set_up();
    $self->{instance} = Cassandane::Instance->new(setup_mailbox => 0);
}

sub tear_down
{
    my ($self) = @_;
    $self->lemming_cull();
    $self->SUPER::tear_down();
}

sub lemming_connect
{
    my ($srv) = @_;

    my $sock = create_client_socket(
		    $srv->address_family(),
		    $srv->{host}, $srv->{port})
	or die "Cannot connect to lemming " . $srv->address() . ": $@";

    # The lemming sends us his PID so we can later wait for him to die
    # properly.  It's easiest for synchronisation purposes to encode
    # this as a fixed sized field.
    my $pid;
    $sock->sysread($pid, 4)
	or die "Cannot read from lemming: $!";
    $pid = unpack("L", $pid);
    die "Cannot read from lemming: $!"
	unless defined $pid;

    return { sock => $sock, pid => $pid };
}

sub lemming_push
{
    my ($lemming, $mode) = @_;

#     xlog "Pushing mode=$mode to pid=$lemming->{pid}";

    # Push the lemming over the metaphorical cliff.
    $lemming->{sock}->syswrite($mode . "\r\n");
    $lemming->{sock}->close();

    # Wait for the master process to wake up and reap the lemming.
    timed_wait(sub { kill(0, $lemming->{pid}) == 0 },
	       description => "master to reap lemming $lemming->{pid}");
}

sub lemming_census
{
    my ($self) = @_;
    my $coresdir = $self->{instance}->{basedir} . '/conf/cores';

    my %pids;
    opendir LEMM,$coresdir
	or die "cannot open $coresdir for reading: $!";
    while ($_ = readdir LEMM)
    {
	my ($tag, $pid) = m/^lemming\.(\w+).(\d+)$/;
	next
	    unless defined $pid;
	xlog "found lemming tag=$tag pid=$pid";
	$pids{$tag} = []
	    unless defined $pids{$tag};
	push (@{$pids{$tag}}, $pid);
    }
    closedir LEMM;

    my %actual;
    foreach my $tag (keys %pids)
    {
	my $ntotal = scalar @{$pids{$tag}};
	my $nlive = kill(0, @{$pids{$tag}});
	$actual{$tag} = {
	    live => $nlive,
	    dead => $ntotal - $nlive,
	};
    }
    return \%actual;
}

sub lemming_cull
{
    my ($self) = @_;
    return unless defined $self->{instance};
    my $coresdir = $self->{instance}->{basedir} . '/conf/cores';

    return unless -d $coresdir;
    opendir LEMM,$coresdir
	or die "cannot open $coresdir for reading: $!";
    while ($_ = readdir LEMM)
    {
	my ($tag, $pid) = m/^lemming\.(\w+).(\d+)$/;
	next
	    unless defined $pid;
	xlog "culled lemming tag=$tag pid=$pid"
	    if kill(9, $pid);
    }
    closedir LEMM;
}

sub _lemming_args
{
    my (%params) = @_;

    my $tag = delete $params{tag} || 'A';
    my $mode = delete $params{mode} || 'serve';
    my $delay = delete $params{delay};

    my @argv = ( $lemming_bin, '-t', $tag, '-m', $mode );
    push(@argv, '-d', $delay) if defined $delay;

    return (name => $tag, argv => \@argv, %params);
}

sub lemming_service
{
    my ($self, %params) = @_;
    return $self->{instance}->add_service(_lemming_args(%params));
}

sub lemming_start
{
    my ($self, %params) = @_;
    return $self->{instance}->add_start(_lemming_args(%params));
}

sub lemming_event
{
    my ($self, %params) = @_;
    return $self->{instance}->add_event(_lemming_args(%params));
}

sub lemming_wait
{
    my ($self, %expected) = @_;

    timed_wait(
	sub
	{
	    my $census = $self->lemming_census();
	    map {
		return 0 if !defined $census->{$_};
		return 0 if $census->{$_}->{live} != $expected{$_};
	    } keys %expected;
	    return 1;
	},
	description => "master to prefork the configured lemmings");
}

sub start
{
    my ($self) = @_;
    $self->{instance}->start();
}

#
# Test a single running programs in SERVICES
#
sub test_service
{
    my ($self) = @_;

    xlog "single successful service";
    my $srv = $self->lemming_service();
    $self->start();

    xlog "not preforked, so no lemmings running yet";
    $self->assert_deep_equals({},
			      $self->lemming_census());

    my $lemm = lemming_connect($srv);

    xlog "connected so one lemming forked";
    $self->assert_deep_equals({ A => { live => 1, dead => 0 } },
			      $self->lemming_census());

    lemming_push($lemm, 'success');

    xlog "no more live lemmings";
    $self->assert_deep_equals({ A => { live => 0, dead => 1 } },
			      $self->lemming_census());
}

#
# Test multiple connections to a single running program in SERVICES
#
sub test_multi_connections
{
    my ($self) = @_;

    xlog "multiple connections to a single successful service";
    my $srv = $self->lemming_service();
    $self->start();

    xlog "not preforked, so no lemmings running yet";
    $self->assert_deep_equals({},
			      $self->lemming_census());

    my $lemm1 = lemming_connect($srv);

    xlog "connected so one lemming forked";
    $self->assert_deep_equals({ A => { live => 1, dead => 0 } },
			      $self->lemming_census());

    my $lemm2 = lemming_connect($srv);

    xlog "two connected so two lemmings forked";
    $self->assert_deep_equals({ A => { live => 2, dead => 0 } },
			      $self->lemming_census());

    my $lemm3 = lemming_connect($srv);

    xlog "three connected so three lemmings forked";
    $self->assert_deep_equals({ A => { live => 3, dead => 0 } },
			      $self->lemming_census());

    lemming_push($lemm1, 'success');
    lemming_push($lemm2, 'success');
    lemming_push($lemm3, 'success');

    xlog "no more live lemmings";
    $self->assert_deep_equals({ A => { live => 0, dead => 3 } },
			      $self->lemming_census());
}

#
# Test multiple running programs in SERVICES
#
sub test_multi_services
{
    my ($self) = @_;

    xlog "multiple successful services";
    my $srvA = $self->lemming_service(tag => 'A');
    my $srvB = $self->lemming_service(tag => 'B');
    my $srvC = $self->lemming_service(tag => 'C');
    $self->start();

    xlog "not preforked, so no lemmings running yet";
    $self->assert_deep_equals({},
			      $self->lemming_census());

    my $lemmA = lemming_connect($srvA);

    xlog "connected so one lemming forked";
    $self->assert_deep_equals({ A =>  { live => 1, dead => 0 } },
			      $self->lemming_census());

    my $lemmB = lemming_connect($srvB);

    xlog "two connected so two lemmings forked";
    $self->assert_deep_equals({
				A => { live => 1, dead => 0 },
				B => { live => 1, dead => 0 },
			      }, $self->lemming_census());

    my $lemmC = lemming_connect($srvC);

    xlog "three connected so three lemmings forked";
    $self->assert_deep_equals({
				A => { live => 1, dead => 0 },
				B => { live => 1, dead => 0 },
				C => { live => 1, dead => 0 },
			      }, $self->lemming_census());

    lemming_push($lemmA, 'success');
    lemming_push($lemmB, 'success');
    lemming_push($lemmC, 'success');

    xlog "no more live lemmings";
    $self->assert_deep_equals({
				A => { live => 0, dead => 1 },
				B => { live => 0, dead => 1 },
				C => { live => 0, dead => 1 },
			      }, $self->lemming_census());
}

#
# Test a preforked single running program in SERVICES
#
sub test_prefork
{
    my ($self) = @_;

    xlog "single successful service";
    my $srv = $self->lemming_service(prefork => 1);
    $self->start();
    $self->lemming_wait(A => 1);

    xlog "preforked, so one lemming running already";
    $self->assert_deep_equals({ A => { live => 1, dead => 0 } },
			      $self->lemming_census());

    my $lemm1 = lemming_connect($srv);
    $self->lemming_wait(A => 2);

    xlog "connected so one lemming forked";
    $self->assert_deep_equals({ A => { live => 2, dead => 0 } },
			      $self->lemming_census());

    my $lemm2 = lemming_connect($srv);
    $self->lemming_wait(A => 3);

    xlog "connected again so two additional lemmings forked";
    $self->assert_deep_equals({ A => { live => 3, dead => 0 } },
			      $self->lemming_census());

    lemming_push($lemm1, 'success');
    lemming_push($lemm2, 'success');

    xlog "always at least one live lemming";
    $self->assert_deep_equals({ A => { live => 1, dead => 2 } },
			      $self->lemming_census());
}

#
# Test multiple running programs in SERVICES, some preforked.
#
sub test_multi_prefork
{
    my ($self) = @_;

    xlog "multiple successful service some preforked";
    my $srvA = $self->lemming_service(tag => 'A', prefork => 2);
    my $srvB = $self->lemming_service(tag => 'B'); # no preforking
    my $srvC = $self->lemming_service(tag => 'C', prefork => 3);
    $self->start();

    # wait for lemmings to be preforked
    $self->lemming_wait(A => 2, C => 3);

    my @lemmings;
    my $lemm;

    xlog "connect to A once";
    $lemm = lemming_connect($srvA);
    $self->lemming_wait(A => 3);
    push(@lemmings, $lemm);
    $self->assert_deep_equals({
				A => { live => 3, dead => 0 },
				C => { live => 3, dead => 0 },
			      }, $self->lemming_census());

    xlog "connect to A again";
    $lemm = lemming_connect($srvA);
    $self->lemming_wait(A => 4);
    push(@lemmings, $lemm);
    $self->assert_deep_equals({
				A => { live => 4, dead => 0 },
				C => { live => 3, dead => 0 },
			      }, $self->lemming_census());

    xlog "connect to A a third time";
    $lemm = lemming_connect($srvA);
    $self->lemming_wait(A => 5);
    push(@lemmings, $lemm);
    $self->assert_deep_equals({
				A => { live => 5, dead => 0 },
				C => { live => 3, dead => 0 },
			      }, $self->lemming_census());

    xlog "connect to B";
    $lemm = lemming_connect($srvB);
    push(@lemmings, $lemm);
    $self->assert_deep_equals({
				A => { live => 5, dead => 0 },
				B => { live => 1, dead => 0 },
				C => { live => 3, dead => 0 },
			      }, $self->lemming_census());

    foreach $lemm (@lemmings)
    {
	lemming_push($lemm, 'success');
    }

    xlog "our lemmings are gone, others have replaced them";
    $self->assert_deep_equals({
				A => { live => 2, dead => 3 },
				B => { live => 0, dead => 1 },
				C => { live => 3, dead => 0 },
			      }, $self->lemming_census());
}

#
# Test a single program in SERVICES which fails after connect
#
sub test_exit_after_connect
{
    my ($self) = @_;

    xlog "single service will exit after connect";
    my $srv = $self->lemming_service();
    $self->start();

    xlog "not preforked, so no lemmings running yet";
    $self->assert_deep_equals({},
			      $self->lemming_census());

    my $lemm = lemming_connect($srv);

    xlog "connected so one lemming forked";
    $self->assert_deep_equals({ A => { live => 1, dead => 0 } },
			      $self->lemming_census());

    xlog "push the lemming off the cliff";
    lemming_push($lemm, 'exit');
    $self->assert_deep_equals({ A => { live => 0, dead => 1 } },
			      $self->lemming_census());

    xlog "can connect again";
    $lemm = lemming_connect($srv);
    $self->assert_deep_equals({ A => { live => 1, dead => 1 } },
			      $self->lemming_census());

    xlog "push the lemming off the cliff";
    lemming_push($lemm, 'exit');
    $self->assert_deep_equals({ A => { live => 0, dead => 2 } },
			      $self->lemming_census());
}

#
# Test a single program in SERVICES which fails during startup
#
sub test_service_exit_during_start
{
    my ($self) = @_;
    my $lemm;

    xlog "single service will exit during startup";
    my $srv = $self->lemming_service(mode => 'exit', delay => 100);
    $self->start();

    xlog "not preforked, so no lemmings running yet";
    $self->assert_deep_equals({},
			      $self->lemming_census());

    xlog "connection fails due to dead lemming";
    eval
    {
	$lemm = lemming_connect($srv);
    };
    $self->assert_null($lemm);

    xlog "expect 5 dead lemmings";
    $self->assert_deep_equals({ A => { live => 0, dead => 5 } },
			      $self->lemming_census());

    xlog "connections should fail because service disabled";
    eval
    {
	$lemm = lemming_connect($srv);
    };
    $self->assert_null($lemm);
    $self->assert_deep_equals({ A => { live => 0, dead => 5 } },
			      $self->lemming_census());
}

sub test_startup
{
    my ($self) = @_;

    xlog "Test a program in the START section";
    $self->lemming_start(tag => 'A', delay => 100, mode => 'success');
    $self->lemming_start(tag => 'B', delay => 200, mode => 'success');
    # This service won't be used
    my $srv = $self->lemming_service(tag => 'C');
    $self->start();

    xlog "expect 2 dead lemmings";
    $self->assert_deep_equals({
				A => { live => 0, dead => 1 },
				B => { live => 0, dead => 1 },
			      }, $self->lemming_census());
}

sub test_startup_exits
{
    my ($self) = @_;

    xlog "Test a program in the START section which fails";
    $self->lemming_start(tag => 'A', delay => 100, mode => 'exit');
    $self->lemming_start(tag => 'B', delay => 200, mode => 'exit');
    # This service won't be used
    my $srv = $self->lemming_service(tag => 'C');
    eval
    {
	$self->start();
    };
    xlog "start failed (as expected): $@" if $@;

    xlog "master should have exited when first startup failed";
    $self->assert(!$self->{instance}->is_running());

    xlog "expect 1 dead lemming";
    $self->assert_deep_equals({
				A => { live => 0, dead => 1 },
			      }, $self->lemming_census());
}

# TODO: test exit during startup with prefork=

sub test_service_ipv6
{
    my ($self) = @_;

    xlog "single successful service on IPv6";
    my $srv = $self->lemming_service(host => '::1');
    $self->start();

    xlog "not preforked, so no lemmings running yet";
    $self->assert_deep_equals({},
			      $self->lemming_census());

    my $lemm = lemming_connect($srv);

    xlog "connected so one lemming forked";
    $self->assert_deep_equals({ A => { live => 1, dead => 0 } },
			      $self->lemming_census());

    lemming_push($lemm, 'success');

    xlog "no more live lemmings";
    $self->assert_deep_equals({ A => { live => 0, dead => 1 } },
			      $self->lemming_census());
}

sub test_service_unix
{
    my ($self) = @_;

    xlog "single successful service on UNIX domain socket";
    my $sockname = $self->{instance}->{basedir} .  "/conf/socket/lemming.sock";
    my $srv = $self->lemming_service(
			host => undef,
			port => $sockname);
    $self->start();

    xlog "not preforked, so no lemmings running yet";
    $self->assert_deep_equals({},
			      $self->lemming_census());

    my $lemm = lemming_connect($srv);

    xlog "connected so one lemming forked";
    $self->assert_deep_equals({ A => { live => 1, dead => 0 } },
			      $self->lemming_census());

    lemming_push($lemm, 'success');

    xlog "no more live lemmings";
    $self->assert_deep_equals({ A => { live => 0, dead => 1 } },
			      $self->lemming_census());
}

sub test_service_nohost
{
    my ($self) = @_;

    xlog "single successful service with a port-only listen=";
    my $srv = $self->lemming_service(host => undef);
    $self->start();

    xlog "not preforked, so no lemmings running yet";
    $self->assert_deep_equals({},
			      $self->lemming_census());

    my $lemm = lemming_connect($srv);

    xlog "connected so one lemming forked";
    $self->assert_deep_equals({ A => { live => 1, dead => 0 } },
			      $self->lemming_census());

    lemming_push($lemm, 'success');

    xlog "no more live lemmings";
    $self->assert_deep_equals({ A => { live => 0, dead => 1 } },
			      $self->lemming_census());
}

1;
