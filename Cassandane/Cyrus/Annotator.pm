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
package Cassandane::Cyrus::Annotator;
use base qw(Cassandane::Cyrus::TestCase);
use Cwd qw(abs_path);
use Cassandane::Util::Log;
use Cassandane::Util::Wait;

sub new
{
    my $class = shift;
    my $config = Cassandane::Config->default()->clone();
    $config->set(
	annotation_callout => '@basedir@/conf/socket/annotator.sock',
	annotation_allow_undefined => 1,
    );
    return $class->SUPER::new({
	config => $config,
	deliver => 1,
	adminstore => 1,
    }, @_);
}

sub set_up
{
    my ($self) = @_;
    $self->SUPER::set_up();

    my $daemon = abs_path('utils/annotator.pl');
    my $sock = $self->{instance}->{basedir} . '/conf/socket/annotator.sock';
    my $pidfile = $self->{instance}->{basedir} . '/conf/socket/annotator.pid';
    # Start daemon
    $self->{instance}->run_command({ cyrus => 1 }, $daemon);
    timed_wait(sub { return ( -e $sock ); },
	       description => 'annotator daemon to be ready');
    $self->{annotator_pidfile} = $pidfile;
}

sub tear_down
{
    my ($self) = @_;

    if (defined $self->{annotator_pidfile})
    {
	# Stop daemon
	$self->{instance}->stop_command_pidfile($self->{annotator_pidfile});
	$self->{annotator_pidfile} = undef;
    }

    $self->SUPER::tear_down();
}

sub test_add_annot_deliver
{
    my ($self) = @_;

    my $entry = '/comment';
    my $attrib = 'value.shared';
    # Data thanks to http://hipsteripsum.me
    my $value1 = 'you_probably_havent_heard_of_them';

    my %exp;
    $exp{A} = $self->{gen}->generate(subject => "Message A");
    $exp{A}->set_body("set_shared_annotation $entry $value1\r\n");
    $self->{instance}->deliver($exp{A});
    $exp{A}->set_annotation($entry, $attrib, $value1);

    # Local delivery adds headers we can't predict or control,
    # which change the SHA1 of delivered messages, so we can't
    # be checking the GUIDs here.
    $self->{store}->set_fetch_attributes('uid', "annotation ($entry $attrib)");
    $self->check_messages(\%exp, check_guid => 0);
}

sub test_add_annot_deliver_tomailbox
{
    my ($self) = @_;

    xlog "Testing adding an annotation from the Annotator";
    xlog "when delivering to a non-INBOX mailbox [IRIS-955]";

    my $entry = '/comment';
    my $attrib = 'value.shared';
    # Data thanks to http://hipsteripsum.me
    my $value1 = 'before_they_sold_out';

    my $subfolder = 'target';
    my $talk = $self->{store}->get_client();
    $talk->create("INBOX.$subfolder")
	or die "Failed to create INBOX.$subfolder";

    my %exp;
    $exp{A} = $self->{gen}->generate(subject => "Message A");
    $exp{A}->set_body("set_shared_annotation $entry $value1\r\n");
    $self->{instance}->deliver($exp{A}, folder => $subfolder);
    $exp{A}->set_annotation($entry, $attrib, $value1);

    # Local delivery adds headers we can't predict or control,
    # which change the SHA1 of delivered messages, so we can't
    # be checking the GUIDs here.
    $self->{store}->set_folder("INBOX.$subfolder");
    $self->{store}->set_fetch_attributes('uid', "annotation ($entry $attrib)");
    $self->check_messages(\%exp, check_guid => 0);
}

sub test_set_system_flag_deliver
{
    my ($self) = @_;

    my $flag = '\\Flagged';

    my %exp;
    $exp{A} = $self->{gen}->generate(subject => "Message A");
    $exp{A}->set_body("set_flag $flag\r\n");
    $self->{instance}->deliver($exp{A});
    $exp{A}->set_attributes(flags => ['\\Recent', $flag]);

    # Local delivery adds headers we can't predict or control,
    # which change the SHA1 of delivered messages, so we can't
    # be checking the GUIDs here.
    $self->{store}->set_fetch_attributes('uid', 'flags');
    $self->check_messages(\%exp, check_guid => 0);
}

sub test_set_user_flag_deliver
{
    my ($self) = @_;

    # Data thanks to http://hipsteripsum.me
    my $flag = '$Artisanal';

    my %exp;
    $exp{A} = $self->{gen}->generate(subject => "Message A");
    $exp{A}->set_body("set_flag $flag\r\n");
    $self->{instance}->deliver($exp{A});
    $exp{A}->set_attributes(flags => ['\\Recent', $flag]);

    # Local delivery adds headers we can't predict or control,
    # which change the SHA1 of delivered messages, so we can't
    # be checking the GUIDs here.
    $self->{store}->set_fetch_attributes('uid', 'flags');
    $self->check_messages(\%exp, check_guid => 0);
}

# Note: clear_{shared,private}_annotation can't really be tested with local
# delivery, just with the APPEND command.

sub get_highestmodseq
{
    my ($self) = @_;

    my $imaptalk = $self->{store}->get_client();
    my $status = $imaptalk->status($self->{store}->{folder}, '(highestmodseq)')
	or die "Cannot get status: $@";
    $self->assert_not_null($status);
    $self->assert_not_null($status->{highestmodseq});
    return $status->{highestmodseq};
}

sub test_xrunannotator
{
    my ($self) = @_;

    xlog "Test the XRUNANNOTATOR command [IRIS-1028]";

    my $entry1 = '/vendor/hipsteripsum.me/buzzword';
    my $attrib1 = 'value.shared';
    my @values1 = qw(dummy
		     cred hoodie tattooed jean shorts
		     wayfarers cosby sweater gluten-free aesthetic
		     vinyl ethical retro tumblr synth
		     craft beer bicycle rights etsy);
    my $flag = '$X-Annotated';

    my $entry2 = '/comment';
    my $attrib2 = 'value.shared';
    # Data thanks to http://hipsteripsum.me
    my $value2 = 'squid_letterpress_sustainable';

    xlog "Ensure the mailbox has both w and n rights";
    my $admintalk = $self->{adminstore}->get_client();
    $admintalk->setacl("user.cassandane", "cassandane", '+wn');

    my $imaptalk = $self->{store}->get_client();
    $self->{store}->set_fetch_attributes('uid', 'flags', 'modseq',
					 "annotation ($entry1 $attrib1)",
					 "annotation ($entry2 $attrib2)");

    xlog "Create some messages";
    my %msg;
    for (1..20)
    {
	$msg{$_} = $self->make_message("Message $_",
		uid => $_,
		# the body contains instructions for the annotator
		body => "set_shared_annotation $entry1 $values1[$_]\r\n" .
			"set_flag $flag\r\n");
	$msg{$_}->set_attributes(flags => []);
	$msg{$_}->set_annotation($entry1, $attrib1, undef);
	$msg{$_}->set_annotation($entry2, $attrib2, undef);
    }

    # Some additional preconditions

    xlog "Message 5 has a pre-existing shared annotation";
    $imaptalk->store('5', 'annotation',
		     [$entry2, [$attrib2, { Quote => $value2 }]])
	or die "Cannot set annotation $entry2: $@";
    $msg{5}->set_annotation($entry2, $attrib2, $value2);

    xlog "Messages 7 and 8 have pre-existing flags";
    $imaptalk->store('7', '+flags', "\\Flagged")
	or die "Cannot set \\Flagged: $@";
    $msg{7}->set_attributes(flags => ["\\Flagged"]);
    $imaptalk->store('8', '+flags', "\\Answered")
	or die "Cannot set \\Answered: $@";
    $msg{8}->set_attributes(flags => ["\\Answered"]);

    # We just APPENDed a bunch of messages...which of course ran the
    # annotator and now we have $flag and $entry1 everywhere.  Need
    # to remove them so we can tell whether the annotator ran.  Also
    # as this bumps modseq on every message we have a predictable
    # modseq state for each message which we can check later.
    xlog "Clear the annotator's spoor";
    $imaptalk->store('1:*', '-flags', $flag)
	or die "Cannot clear $flag: $@";
    $imaptalk->store('1:*', 'annotation',
		     [$entry1, [$attrib1, undef]])
	or die "Cannot clear annotation $entry1: $@";
    my $modseq0 = $self->get_highestmodseq();

    xlog "Messages are present and correct on the server";
    for (1..20)
    {
	$msg{$_}->set_attribute(modseq => [ $modseq0 ]);
    }
    $self->check_messages(\%msg);

    xlog "Tell the server to run the annotator on messages 3 to 11";
    my $res = $imaptalk->_imap_cmd('xrunannotator', 1, 'fetch', '3:11')
	or die "Cannot issue xrunannotator: $@";
    # Check the returned FETCH results from XRUNANNOTATOR
    $self->assert_deep_equals($res, {
	3 => { flags => [ $flag ], modseq => [ $modseq0+1 ] },
	4 => { flags => [ $flag ], modseq => [ $modseq0+1 ] },
	5 => { flags => [ $flag ], modseq => [ $modseq0+1 ] },
	6 => { flags => [ $flag ], modseq => [ $modseq0+1 ] },
	7 => { flags => [ "\\Flagged", $flag ], modseq => [ $modseq0+1 ] },
	8 => { flags => [ "\\Answered", $flag ], modseq => [ $modseq0+1 ] },
	9 => { flags => [ $flag ], modseq => [ $modseq0+1 ] },
	10 => { flags => [ $flag ], modseq => [ $modseq0+1 ] },
	11 => { flags => [ $flag ], modseq => [ $modseq0+1 ] },
    });

    xlog "Messages have the expected new annotation and flags and modseq";
    for (3..11)
    {
	# All the messages in range should have the new entry
	$msg{$_}->set_annotation($entry1, $attrib1, $values1[$_]);
	# All the messages in range should have the new flag
	my $flags = $msg{$_}->get_attribute('flags');
	push(@$flags, $flag);
	# Note that we expect XRUNANNOTATOR to bump modseq by exactly 1
	# and only for the messages it's given
	$msg{$_}->set_attributes(flags => $flags, modseq => [ $modseq0+1 ]);
    }
    $self->check_messages(\%msg);
}


1;
