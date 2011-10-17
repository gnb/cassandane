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
package Cassandane::Cyrus::Conversations;
use base qw(Cassandane::Cyrus::TestCase);
use DateTime;
use URI::Escape;
use Digest::SHA1 qw(sha1_hex);
use Cassandane::ThreadedGenerator;
use Cassandane::Message;
use Cassandane::Util::Log;
use Cassandane::Util::DateTime qw(to_iso8601 from_iso8601
				  from_rfc822
				  to_rfc3501 from_rfc3501);

sub new
{
    my ($class, @args) = @_;
    my $config = Cassandane::Config->default()->clone();
    $config->set(conversations => 'on');
    return $class->SUPER::new({ config => $config }, @args);
}

sub set_up
{
    my ($self) = @_;
    $self->SUPER::set_up();
    $self->{store}->set_fetch_attributes('uid', 'cid');
}

sub tear_down
{
    my ($self) = @_;
    $self->SUPER::tear_down();
}

# The resulting CID when a clash happens is supposed to be
# the MAXIMUM of all the CIDs.  Here we use the fact that
# CIDs are expressed in a form where lexical order is the
# same as numeric order.
sub choose_cid
{
    my (@cids) = @_;
    @cids = sort { $b cmp $a } @cids;
    return $cids[0];
}

#
# Test APPEND of messages to IMAP
#
sub test_append
{
    my ($self) = @_;
    my %exp;

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($self->{store}->get_client()->capability()->{xconversations});

    xlog "generating message A";
    $exp{A} = $self->make_message("Message A");
    $exp{A}->set_attributes(uid => 1, cid => $exp{A}->make_cid());
    $self->check_messages(\%exp);

    xlog "generating message B";
    $exp{B} = $self->make_message("Message B");
    $exp{B}->set_attributes(uid => 2, cid => $exp{B}->make_cid());
    $self->check_messages(\%exp);

    xlog "generating message C";
    $exp{C} = $self->make_message("Message C");
    $exp{C}->set_attributes(uid => 3, cid => $exp{C}->make_cid());
    my $actual = $self->check_messages(\%exp);

    xlog "generating message D";
    $exp{D} = $self->make_message("Message D");
    $exp{D}->set_attributes(uid => 4, cid => $exp{D}->make_cid());
    $self->check_messages(\%exp);
}


#
# Test APPEND of messages to IMAP which results in a CID clash.
#
sub test_append_clash
{
    my ($self) = @_;
    my %exp;

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($self->{store}->get_client()->capability()->{xconversations});

    xlog "generating message A";
    $exp{A} = $self->make_message("Message A");
    $exp{A}->set_attributes(uid => 1, cid => $exp{A}->make_cid());
    $self->check_messages(\%exp);

    xlog "generating message B";
    $exp{B} = $self->make_message("Message B");
    $exp{B}->set_attributes(uid => 2, cid => $exp{B}->make_cid());
    my $actual = $self->check_messages(\%exp);

    xlog "generating message C";
    my $ElCid = choose_cid($exp{A}->get_attribute('cid'),
			   $exp{B}->get_attribute('cid'));
    $exp{C} = $self->make_message("Message C",
				  references =>
				       $exp{A}->get_header('message-id') .  ", " .
				       $exp{B}->get_header('message-id'),
				 );
    $exp{C}->set_attributes(uid => 3, cid => $ElCid);

    # Since IRIS-293, inserting this message will have the side effect
    # of renumbering some of the existing messages.  Predict and test
    # which messages get renumbered.
    my $nextuid = 4;
    foreach my $s (qw(A B))
    {
	if ($actual->{"Message $s"}->make_cid() ne $ElCid)
	{
	    $exp{$s}->set_attributes(uid => $nextuid, cid => $ElCid);
	    $nextuid++;
	}
    }

    $self->check_messages(\%exp);
}

#
# Test APPEND of messages to IMAP which results in multiple CID clashes.
#
sub test_double_clash
{
    my ($self) = @_;
    my %exp;

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($self->{store}->get_client()->capability()->{xconversations});

    xlog "generating message A";
    $exp{A} = $self->make_message("Message A");
    $exp{A}->set_attributes(uid => 1, cid => $exp{A}->make_cid());
    $self->check_messages(\%exp);

    xlog "generating message B";
    $exp{B} = $self->make_message("Message B");
    $exp{B}->set_attributes(uid => 2, cid => $exp{B}->make_cid());
    $self->check_messages(\%exp);

    xlog "generating message C";
    $exp{C} = $self->make_message("Message C");
    $exp{C}->set_attributes(uid => 3, cid => $exp{C}->make_cid());
    my $actual = $self->check_messages(\%exp);

    xlog "generating message D";
    my $ElCid = choose_cid($exp{A}->get_attribute('cid'),
			   $exp{B}->get_attribute('cid'),
			   $exp{C}->get_attribute('cid'));
    $exp{D} = $self->make_message("Message D",
				  references =>
				       $exp{A}->get_header('message-id') .  ", " .
				       $exp{B}->get_header('message-id') .  ", " .
				       $exp{C}->get_header('message-id'),
				 );
    $exp{D}->set_attributes(uid => 4, cid => $ElCid);

    # Since IRIS-293, inserting this message will have the side effect
    # of renumbering some of the existing messages.  Predict and test
    # which messages get renumbered.
    my $nextuid = 5;
    foreach my $s (qw(A B C))
    {
	if ($actual->{"Message $s"}->make_cid() ne $ElCid)
	{
	    $exp{$s}->set_attributes(uid => $nextuid, cid => $ElCid);
	    $nextuid++;
	}
    }

    $self->check_messages(\%exp);
}

#
# Test that a CID clash resolved on the master is replicated
#
sub test_replication_clash
{
    my ($self) = @_;
    my %exp;

    xlog "need a master and replica pair";
    $self->assert_not_null($self->{replica});
    my $master_store = $self->{master_store};
    my $replica_store = $self->{replica_store};

    $master_store->set_fetch_attributes('uid', 'cid');
    $replica_store->set_fetch_attributes('uid', 'cid');

    # Double check that we're connected to the servers
    # we wanted to be connected to.
    $self->assert($master_store->{host} eq $replica_store->{host});
    $self->assert($master_store->{port} != $replica_store->{port});

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($master_store->get_client()->capability()->{xconversations});
    $self->assert($replica_store->get_client()->capability()->{xconversations});

    xlog "generating message A";
    $exp{A} = $self->make_message("Message A", store => $master_store);
    $exp{A}->set_attributes(uid => 1, cid => $exp{A}->make_cid());
    $self->run_replication();
    $self->check_messages(\%exp, store => $master_store);
    $self->check_messages(\%exp, store => $replica_store);

    xlog "generating message B";
    $exp{B} = $self->make_message("Message B", store => $master_store);
    $exp{B}->set_attributes(uid => 2, cid => $exp{B}->make_cid());
    $self->run_replication();
    $self->check_messages(\%exp, store => $master_store);
    $self->check_messages(\%exp, store => $replica_store);

    xlog "generating message C";
    $exp{C} = $self->make_message("Message C", store => $master_store);
    $exp{C}->set_attributes(uid => 3, cid => $exp{C}->make_cid());
    $self->run_replication();
    my $actual = $self->check_messages(\%exp, store => $master_store);
    $self->check_messages(\%exp, store => $replica_store);

    xlog "generating message D";
    my $ElCid = choose_cid($exp{A}->get_attribute('cid'),
			   $exp{B}->get_attribute('cid'),
			   $exp{C}->get_attribute('cid'));
    $exp{D} = $self->make_message("Message D",
				  store => $master_store,
				  references =>
				       $exp{A}->get_header('message-id') .  ", " .
				       $exp{B}->get_header('message-id') .  ", " .
				       $exp{C}->get_header('message-id')
				 );
    $exp{D}->set_attributes(uid => 4, cid => $ElCid);

    # Since IRIS-293, inserting this message will have the side effect
    # of renumbering some of the existing messages.  Predict and test
    # which messages get renumbered.
    my $nextuid = 5;
    foreach my $s (qw(A B C))
    {
	if ($actual->{"Message $s"}->make_cid() ne $ElCid)
	{
	    $exp{$s}->set_attributes(uid => $nextuid, cid => $ElCid);
	    $nextuid++;
	}
    }

    $self->run_replication();
    $self->check_messages(\%exp, store => $master_store);
    $self->check_messages(\%exp, store => $replica_store);
}

sub test_xconvfetch
{
    my ($self) = @_;
    my $store = $self->{store};

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($store->get_client()->capability()->{xconversations});

    xlog "generating messages";
    my $generator = Cassandane::ThreadedGenerator->new();
    $store->write_begin();
    while (my $msg = $generator->generate())
    {
	$store->write_message($msg);
    }
    $store->write_end();

    xlog "reading the whole folder again to discover CIDs etc";
    my %cids;
    my %uids;
    $store->read_begin();
    while (my $msg = $store->read_message())
    {
	my $uid = $msg->get_attribute('uid');
	my $cid = $msg->get_attribute('cid');
	my $threadid = $msg->get_header('X-Cassandane-Thread');
	if (defined $cids{$cid})
	{
	    $self->assert_num_equals($threadid, $cids{$cid});
	}
	else
	{
	    $cids{$cid} = $threadid;
	    xlog "Found CID $cid";
	}
	$self->assert_null($uids{$uid});
	$uids{$uid} = 1;
    }
    $store->read_end();

    xlog "Using XCONVFETCH on each conversation";
    foreach my $cid (keys %cids)
    {
	xlog "XCONVFETCHing CID $cid";

	my $result = $store->xconvfetch_begin($cid);
	$self->assert_not_null($result->{xconvmeta});
	$self->assert_num_equals(1, scalar keys %{$result->{xconvmeta}});
	$self->assert_not_null($result->{xconvmeta}->{$cid});
	$self->assert_not_null($result->{xconvmeta}->{$cid}->{modseq});
	while (my $msg = $store->xconvfetch_message())
	{
	    my $muid = $msg->get_attribute('uid');
	    my $mcid = $msg->get_attribute('cid');
	    my $threadid = $msg->get_header('X-Cassandane-Thread');
	    $self->assert_str_equals($cid, $mcid);
	    $self->assert_num_equals($cids{$cid}, $threadid);
	    $self->assert_num_equals(1, $uids{$muid});
	    $uids{$muid} |= 2;
	}
	$store->xconvfetch_end();
    }

    xlog "checking that all the UIDs in the folder were XCONVFETCHed";
    foreach my $uid (keys %uids)
    {
	$self->assert_num_equals(3, $uids{$uid});
    }
}

#
# Test APPEND of a new composed draft message to the Drafts folder by
# the Fastmail webui, which sets the X-ME-Message-ID header to thread
# conversations but not any of Message-ID, References, or In-Reply-To.
#
sub test_fm_webui_draft
{
    my ($self) = @_;
    my %exp;

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($self->{store}->get_client()->capability()->{xconversations});

    xlog "generating message A";
    $exp{A} = $self->{gen}->generate(subject => 'Draft message A');
    $exp{A}->remove_headers('Message-ID');
#     $exp{A}->add_header('X-ME-Message-ID', '<fake.header@i.am.a.draft>');
    $exp{A}->add_header('X-ME-Message-ID', '<fake1700@fastmail.fm>');
    $exp{A}->set_attribute(cid => $exp{A}->make_cid());

    $self->{store}->write_begin();
    $self->{store}->write_message($exp{A});
    $self->{store}->write_end();
    $self->check_messages(\%exp);

    xlog "generating message B";
    $exp{B} = $exp{A}->clone();
    $exp{B}->set_headers('Subject', 'Draft message B');
    $exp{B}->set_body("Completely different text here\r\n");

    $self->{store}->write_begin();
    $self->{store}->write_message($exp{B});
    $self->{store}->write_end();
    $self->check_messages(\%exp);
}

#
# XCONVSORT and XCONVUPDATES tests
#

sub make_threaded_messages
{
    my ($self, %params) = @_;
    my $store = $self->{store};

    xlog "generating some messages";
    my $gen = Cassandane::ThreadedGenerator->new(%params);
    my %msgs;
    my %cids;

    $store->write_begin();
    while (my $msg = $gen->generate(extra_lines => int(rand(50))))
    {
	$store->write_message($msg);
	$msgs{$msg->get_attribute('uid')} = $msg;
	$cids{$msg->get_attribute('cid')} = 1;
    }
    $store->write_end();

    return (\%msgs, scalar(keys %msgs), scalar(keys %cids));
}

sub expected_uids
{
    my ($msgs, %params) = @_;
    my $conversations = $params{conversations} || 0;
    my $search = $params{search} || sub { return $_; };
    my $sort = $params{sort} || sub { $a->uid <=> $b->uid; };
    my $position = $params{position} || 1;
    my $limit = $params{limit};
    my $anchor = $params{anchor};
    my $offset = $params{offset} || 0;
    my $anchor_pos;
    my %seen;
    my $pos = 1;
    my @res;

    my @subset = values %$msgs;
    @subset = grep $search, @subset;
    @subset = sort $sort @subset;

    foreach my $msg (@subset)
    {
	my $uid = $msg->get_attribute('uid');

	if ($conversations)
	{
	    my $cid = $msg->get_attribute('cid');
	    next if defined $seen{$cid};
	    $seen{$cid} = 1;
	}

	$anchor_pos = $pos
	    if (defined $anchor && $anchor == $uid);

	push(@res, $uid);
	$pos++;
    }

    if (defined $anchor)
    {
	$limit = scalar(@res) unless defined $limit;
	$anchor_pos = scalar(@res)+1 unless defined $anchor_pos;
	$anchor_pos += $offset;
	@res = splice(@res,$anchor_pos-1,$limit);
    }
    elsif (defined $position)
    {
	$limit = scalar(@res) unless defined $limit;
	@res = splice(@res,$position-1,$limit);
    }

    return \@res;
}

sub check_convsort
{
    my ($self, $messages, $res, %params) = @_;
    my $exp = expected_uids($messages, %params);
    xlog "expecting uids: " . join(" ",@$exp);
    $self->assert_deep_equals($exp, $res);
}

sub convsort
{
    my ($talk, @args) = @_;
    return $talk->_imap_cmd('xconvsort', 0, 'sort', @args);
}

sub convsort_postcondition
{
    my ($self, $exp_uidn, $exp_hms, $exp_pos, $exp_total) = @_;

    my $talk = $self->{store}->get_client();

    $self->assert_str_equals('ok', $talk->get_last_completion_response());

    $self->assert_num_equals($exp_uidn,
			     $talk->get_response_code('uidnext'));
    $self->assert_num_equals($exp_hms,
			     $talk->get_response_code('highestmodseq'));
    if (defined $exp_pos)
    {
	$self->assert_num_equals($exp_pos,
				 $talk->get_response_code('position'));
    }
    else
    {
	$self->assert_null($talk->get_response_code('position'));
    }
    $self->assert_num_equals($exp_total,
			     $talk->get_response_code('total'));

    $talk->clear_response_code('uidnext');
    $talk->clear_response_code('highestmodseq');
    $talk->clear_response_code('position');
    $talk->clear_response_code('total');
}

sub test_convsort_window
{
    my ($self) = @_;

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($self->{store}->get_client()->capability()->{xconversations});

    my $folder = 'INBOX';
    my $store = $self->{store};
    my $talk = $store->get_client();

    my ($messages, $nmessages, $nthreads) =
	    $self->make_threaded_messages(nmessages => 100, nthreads => 15);
    my $exp_uidn = $nmessages+1;
    my $exp_hms = $nmessages+4,
    my $res;
    my $exp;
    my $anchor;

    xlog "xconvsort in message mode with no window-args";
    $res = convsort($talk, $folder, ['uid'],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect all the uids in uid order";
    $self->check_convsort($messages, $res);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 1, $nmessages);

    xlog "xconvsort in conversation mode with no other window-args";
    $res = convsort($talk, $folder, ['uid'],
		    ['conversations'],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect the conversation exemplars in uid order";
    $self->check_convsort($messages, $res, conversations => 1);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 1, $nthreads);

    xlog "xconvsort in message mode with POSITION and no LIMIT";
    $res = convsort($talk, $folder, ['uid'],
		    ['position', [5, 0]],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect all the uids from 5 onward in uid order";
    $self->check_convsort($messages, $res, position => 5);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 5, $nmessages);

    xlog "xconvsort in conversation mode with POSITION and no LIMIT";
    $res = convsort($talk, $folder, ['uid'],
		    ['conversations', 'position', [5, 0]],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect all the exemplars from the 5th onward in uid order";
    $self->check_convsort($messages, $res,
			  conversations => 1, position => 5);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 5, $nthreads);

    xlog "xconvsort in message mode with POSITION and LIMIT";
    $res = convsort($talk, $folder, ['uid'],
		    ['position', [5, 7]],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect 7 uids from 5 onward in uid order";
    $self->check_convsort($messages, $res,
			  position => 5, limit => 7);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 5, $nmessages);

    xlog "xconvsort in conversation mode with POSITION and LIMIT";
    $res = convsort($talk, $folder, ['uid'],
		    ['conversations', 'position', [5, 7]],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect 7 exemplars from the 5th onward in uid order";
    $self->check_convsort($messages, $res,
			  conversations => 1, position => 5, limit => 7);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 5, $nthreads);

    xlog "xconvsort in message mode with POSITION and LIMIT=1";
    $res = convsort($talk, $folder, ['uid'],
		    ['position', [5, 1]],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect 1 uids from 5 onward in uid order";
    $self->check_convsort($messages, $res,
			  position => 5, limit => 1);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 5, $nmessages);

    xlog "xconvsort in conversation mode with POSITION and LIMIT=1";
    $res = convsort($talk, $folder, ['uid'],
		    ['conversations', 'position', [5, 1]],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect 1 exemplars from the 5th onward in uid order";
    $self->check_convsort($messages, $res,
			  conversations => 1, position => 5, limit => 1);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 5, $nthreads);

    xlog "xconvsort in message mode with POSITION off the end";
    $res = convsort($talk, $folder, ['uid'],
		    ['position', [$nmessages+1, 7]],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect no uids";
    $self->assert_deep_equals([], $res);
    $self->convsort_postcondition($exp_uidn, $exp_hms, undef, $nmessages);

    xlog "xconvsort in conversation mode with POSITION off the end";
    $res = convsort($talk, $folder, ['uid'],
		    ['conversations', 'position', [$nthreads+1, 7]],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect no exemplars";
    $self->assert_deep_equals([], $res);
    $self->convsort_postcondition($exp_uidn, $exp_hms, undef, $nthreads);

    xlog "xconvsort in message mode with ANCHOR not found";
    $res = convsort($talk, $folder, ['uid'],
		    ['anchor', [$nmessages+1, 0, 0]],
		    'us-ascii', 'all');
    $self->assert_str_equals('no', $talk->get_last_completion_response());
    $self->assert($talk->get_last_error() =~ m/anchor not found/i);

    xlog "xconvsort in conversation mode with ANCHOR not found";
    $res = convsort($talk, $folder, ['uid'],
		    ['conversations', 'anchor', [$nmessages+1, 0, 0]],
		    'us-ascii', 'all');
    $self->assert_str_equals('no', $talk->get_last_completion_response());
    $self->assert($talk->get_last_error() =~ m/anchor not found/i);

    xlog "xconvsort in message mode with ANCHOR and POSITION";
    $res = convsort($talk, $folder, ['uid'],
		    ['anchor', [1, 0, 0], 'position', [1, 0]],
		    'us-ascii', 'all');
    $self->assert_str_equals('bad', $talk->get_last_completion_response());
    $self->assert($talk->get_last_error() =~ m/syntax error in window/i);

    xlog "xconvsort in conversation mode with ANCHOR and POSITION";
    $res = convsort($talk, $folder, ['uid'],
		    ['conversations', 'anchor', [1, 0, 0], 'position', [1, 0]],
		    'us-ascii', 'all');
    $self->assert_str_equals('bad', $talk->get_last_completion_response());
    $self->assert($talk->get_last_error() =~ m/syntax error in window/i);

    xlog "xconvsort in message mode with ANCHOR and no OFFSET or LIMIT";
    $anchor = expected_uids($messages, conversations => 0,
			    position => 5, limit => 1)->[0];
    $res = convsort($talk, $folder, ['uid'],
		    ['anchor', [$anchor, 0, 0]],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect all the uids from 5 onward in uid order";
    $self->check_convsort($messages, $res, anchor => $anchor);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 5, $nmessages);

    xlog "xconvsort in conversation mode with ANCHOR and no OFFSET or LIMIT";
    $anchor = expected_uids($messages, conversations => 1,
			    position => 5, limit => 1)->[0];
    $res = convsort($talk, $folder, ['uid'],
		    ['conversations', 'anchor', [$anchor, 0, 0]],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect all the exemplars from the 5th conversation in uid order";
    $self->check_convsort($messages, $res,
			  conversations => 1, anchor => $anchor);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 5, $nthreads);

    xlog "xconvsort in message mode with ANCHOR and OFFSET but no LIMIT";
    $anchor = expected_uids($messages, conversations => 0,
			    position => 5, limit => 1)->[0];
    $res = convsort($talk, $folder, ['uid'],
		    ['anchor', [$anchor, 3, 0]],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect all the uids from 8 onward in uid order";
    $self->check_convsort($messages, $res,
			  anchor => $anchor, offset => 3);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 8, $nmessages);

    xlog "xconvsort in conversation mode with ANCHOR and OFFSET but no LIMIT";
    $anchor = expected_uids($messages, conversations => 1,
			    position => 5, limit => 1)->[0];
    $res = convsort($talk, $folder, ['uid'],
		    ['conversations', 'anchor', [$anchor, 3, 0]],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect all the exemplars from the 8th conversation in uid order";
    $self->check_convsort($messages, $res,
			  conversations => 1, anchor => $anchor,
			  offset => 3);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 8, $nthreads);

    xlog "xconvsort in message mode with ANCHOR and OFFSET and LIMIT";
    $anchor = expected_uids($messages, conversations => 0,
			    position => 5, limit => 1)->[0];
    $res = convsort($talk, $folder, ['uid'],
		    ['anchor', [$anchor, 3, 7]],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect 7 uids from 8 onward in uid order";
    $self->check_convsort($messages, $res,
			  anchor => $anchor, offset => 3, limit => 7);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 8, $nmessages);

    xlog "xconvsort in conversation mode with ANCHOR and OFFSET and LIMIT";
    $anchor = expected_uids($messages, conversations => 1,
			    position => 5, limit => 1)->[0];
    $res = convsort($talk, $folder, ['uid'],
		    ['conversations', 'anchor', [$anchor, 3, 7]],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect 7 exemplars from the 8th conversation in uid order";
    $self->check_convsort($messages, $res,
			  conversations => 1, anchor => $anchor,
			  offset => 3, limit => 7);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 8, $nthreads);
}

sub from_cmp
{
    my ($afrom) = ($a->from =~ m/<([^@>]*)/);
    my ($bfrom) = ($b->from =~ m/<([^@>]*)/);

    return lc($afrom) cmp lc($bfrom) ||
	   $a->uid <=> $b->uid;
}

sub reverse_from_cmp
{
    my ($afrom) = ($a->from =~ m/<([^@>]*)/);
    my ($bfrom) = ($b->from =~ m/<([^@>]*)/);

    # Note: FROM is REVERSEd but the default UID sort isn't
    return lc($bfrom) cmp lc($afrom) ||
	   $a->uid <=> $b->uid;
}

sub to_cmp
{
    my ($ato) = ($a->to =~ m/<([^@>]*)/);
    my ($bto) = ($b->to =~ m/<([^@>]*)/);

    return lc($ato) cmp lc($bto) ||
	   $a->uid <=> $b->uid;
}

sub subject_cmp
{
    return base_subject($a->subject) cmp base_subject($b->subject) ||
	   $a->uid <=> $b->uid;
}

sub size_cmp
{
    return $a->size <=> $b->size ||
	   $a->uid <=> $b->uid;
}

sub from_reverse_size_subject_cmp
{
    my ($afrom) = ($a->from =~ m/<([^@>]*)/);
    my ($bfrom) = ($b->from =~ m/<([^@>]*)/);

    return lc($afrom) cmp lc($bfrom) ||
	   $b->size <=> $a->size ||
	   base_subject($a->subject) cmp base_subject($b->subject) ||
	   $a->uid <=> $b->uid;
}

sub test_convsort_sort
{
    my ($self) = @_;

    # check IMAP server has the XCONVERSATIONS capability
    $self->assert($self->{store}->get_client()->capability()->{xconversations});

    my $folder = 'INBOX';
    my $store = $self->{store};
    my $talk = $store->get_client();

    my ($messages, $nmessages, $nthreads) =
	    $self->make_threaded_messages(nmessages => 100, nthreads => 15);
    my $exp_uidn = $nmessages+1;
    my $exp_hms = $nmessages+4,
    my $res;
    my $exp;
    my $anchor;

    xlog "xconvsort in message mode sorting on FROM";
    $res = convsort($talk, $folder, ['from'],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect all the uids in FROM order";
    $self->check_convsort($messages, $res, sort => \&from_cmp);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 1, $nmessages);

    xlog "xconvsort in conversation mode sorting on FROM";
    $res = convsort($talk, $folder, ['from'],
		    ['conversations'],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect the conversation exemplars in FROM order";
    $self->check_convsort($messages, $res, conversations => 1,
			  sort => \&from_cmp);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 1, $nthreads);

    xlog "xconvsort in message mode sorting on REVERSE FROM";
    $res = convsort($talk, $folder, ['reverse', 'from'],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect all the uids in REVERSE FROM order";
    $self->check_convsort($messages, $res, sort => \&reverse_from_cmp);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 1, $nmessages);

    xlog "xconvsort in conversation mode sorting on REVERSE FROM";
    $res = convsort($talk, $folder, ['reverse', 'from'],
		    ['conversations'],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect the conversation exemplars in REVERSE FROM order";
    $self->check_convsort($messages, $res, conversations => 1,
			  sort => \&reverse_from_cmp);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 1, $nthreads);

    # TO order isn't very interesting, all the messages are generated
    # with the same To: address anyway.
    xlog "xconvsort in message mode sorting on TO";
    $res = convsort($talk, $folder, ['to'],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect all the uids in TO order";
    $self->check_convsort($messages, $res, sort => \&to_cmp);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 1, $nmessages);

    xlog "xconvsort in conversation mode sorting on TO";
    $res = convsort($talk, $folder, ['to'],
		    ['conversations'],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect the conversation exemplars in TO order";
    $self->check_convsort($messages, $res, conversations => 1,
			  sort => \&to_cmp);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 1, $nthreads);

    xlog "xconvsort in message mode sorting on SUBJECT";
    $res = convsort($talk, $folder, ['subject'],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect all the uids in SUBJECT order";
    $self->check_convsort($messages, $res, sort => \&subject_cmp);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 1, $nmessages);

    xlog "xconvsort in conversation mode sorting on SUBJECT";
    $res = convsort($talk, $folder, ['subject'],
		    ['conversations'],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect the conversation exemplars in SUBJECT order";
    $self->check_convsort($messages, $res, conversations => 1,
			  sort => \&subject_cmp);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 1, $nthreads);

    xlog "xconvsort in message mode sorting on SIZE";
    $res = convsort($talk, $folder, ['size'],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect all the uids in SIZE order";
    $self->check_convsort($messages, $res, sort => \&size_cmp);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 1, $nmessages);

    xlog "xconvsort in conversation mode sorting on SIZE";
    $res = convsort($talk, $folder, ['size'],
		    ['conversations'],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect the conversation exemplars in SIZE order";
    $self->check_convsort($messages, $res, conversations => 1,
			  sort => \&size_cmp);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 1, $nthreads);

    xlog "xconvsort in message mode sorting on FROM REVERSE SIZE SUBJECT";
    $res = convsort($talk, $folder, ['from', 'reverse', 'size', 'subject'],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect all the uids in FROM REVERSE SIZE SUBJECT order";
    $self->check_convsort($messages, $res,
			  sort => \&from_reverse_size_subject_cmp);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 1, $nmessages);

    xlog "xconvsort in conversation mode sorting on FROM REVERSE SIZE
    SUBJECT";
    $res = convsort($talk, $folder, ['from', 'reverse', 'size', 'subject'],
		    ['conversations'],
		    'us-ascii', 'all')
	or die "xconvsort failed: $@";
    xlog "expect the conversation exemplars in FROM REVERSE SIZE SUBJECT order";
    $self->check_convsort($messages, $res, conversations => 1,
			  sort => \&from_reverse_size_subject_cmp);
    $self->convsort_postcondition($exp_uidn, $exp_hms, 1, $nthreads);
}

1;
