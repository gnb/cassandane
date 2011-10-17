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

package Cassandane::Generator;
use strict;
use warnings;
use Cassandane::Util::DateTime qw(to_rfc822);
use Cassandane::Address;
use Cassandane::Message;
use Digest::MD5 qw(md5_hex);
use Digest::SHA1 qw(sha1_hex);

our $admin = 'gnb@fastmail.fm';

our @girls_forenames = (
    # Top 10 girl baby names in 2006 according to
    # http://www.babyhold.com/babynames/Popular/Popular_girl_names_in_the_US_for_2006/
    'Emily',
    'Emma',
    'Madison',
    'Abigail',
    'Olivia',
    'Isabella',
    'Hannah',
    'Samantha',
    'Ava',
    'Ashley'
);
our @surnames = (
    # Top 10 common surnames in Australia according to
    # http://genealogy.about.com/od/australia/tp/common_surnames.htm
    'Smith',
    'Jones',
    'Williams',
    'Brown',
    'Wilson',
    'Taylor',
    'Nguyen',
    'Johnson',
    'Martin',
    'White'
);
our @domains = (
    # Pulled out of my hat.
    'fastmail.fm',
    'gmail.com',
    'hotmail.com',
    'yahoo.com'
);
our @localpart_styles = (
    sub($$$)
    {
	my ($forename, $initial, $surname) = @_;
	return "$forename.$surname";
    },
    sub($$$)
    {
	my ($forename, $initial, $surname) = @_;
	return lc(substr($forename,0,1) . $initial . $surname);
    },
    sub($$$)
    {
	my ($forename, $initial, $surname) = @_;
	return lc(substr($forename,0,1) .  $initial .  substr($surname,0,1));
    }
);

sub new
{
    my ($class, %params) = @_;

    my $self = {
	min_extra_lines => $params{min_extra_lines} || 0,
	max_extra_lines => $params{max_extra_lines} || 0,
    };

    bless $self, $class;
    return $self;
}

sub _make_random_address
{
    my ($self) = @_;

    my $i = int(rand(scalar(@girls_forenames)));
    my $forename = $girls_forenames[$i];

    $i = int(rand(scalar(@surnames)));
    my $surname = $surnames[$i];

    my $digest = md5_hex("$forename $surname");

    $i = oct("0x" . substr($digest,0,4)) % scalar(@domains);
    my $domain = $domains[$i];

    $i = oct("0x" . substr($digest,4,4)) % 26;
    my $initial = substr("ABCDEFGHIJKLMNOPQRSTUVWXYZ", $i, 1);

    $i = oct("0x" . substr($digest,8,4)) % scalar(@localpart_styles);
    my $localpart = $localpart_styles[$i]->($forename, $initial, $surname);

    return Cassandane::Address->new(
	name => "$forename $initial. $surname",
	localpart => $localpart,
	domain => $domain
    );
}

sub _generate_from
{
    my ($self, $params) = @_;
    return $self->_make_random_address();
}

sub _generate_to
{
    my ($self, $params) = @_;
    return Cassandane::Address->new(
	name => "Test User",
	localpart => 'test',
	domain => 'vmtom.com'
    );
}

sub _generate_messageid
{
    my ($self, $params) = @_;
    my $idsalt = int(rand(65536));
    return "fake." . $params->{date}->epoch() . ".$idsalt\@" .  $params->{from}->domain();
}

sub _params_defaults
{
    my $self = shift;
    my $params = { @_ };

    # Note: no error checking, e.g. for unknown parameters.  Sorry.
    #
    $params->{date} = DateTime->now()
	unless defined $params->{date};
    die "Bad date: " . ref $params->{date}
	unless ref $params->{date} eq 'DateTime';

    $params->{from} = $self->_generate_from($params)
	unless defined $params->{from};
    die "Bad from: " . ref $params->{from}
	unless ref $params->{from} eq 'Cassandane::Address';

    $params->{subject} = "Generated test email"
	unless defined $params->{subject};

    $params->{to} = $self->_generate_to($params)
	unless defined $params->{to};
    die "Bad to: " . ref $params->{to}
	unless ref $params->{to} eq 'Cassandane::Address';

    $params->{messageid} = $self->_generate_messageid($params)
	unless defined $params->{messageid};

    $params->{extra_lines} = int($self->{min_extra_lines} +
				 rand($self->{max_extra_lines} -
				      $self->{min_extra_lines}))
	unless defined $params->{extra_lines};

    return $params;
}

sub _generate_unique
{
    return sha1_hex("" . int(rand(65536)));
}

#
# Generate a single email.
# Args: Generator, (param-key => param-value ... )
# Returns: Message ref
#
sub generate
{
    my ($self, @aparams) = @_;
    my $params = $self->_params_defaults(@aparams);
    my $datestr = to_rfc822($params->{date});
    my $from = $params->{from};
    my $to = $params->{to};
    my $extra_lines = $params->{extra_lines};
    my $extra = '';
    if ($extra_lines) {
	$extra .= "This is an extra line\r\n" x $extra_lines;
    }
    my $msg = Cassandane::Message->new();

    $msg->add_header("Return-Path", "<" . $from->address() . ">");
    # TODO: two minutes ago
    $msg->add_header("Received", "from gateway (gateway." . $to->domain() . " [10.0.0.1]) by ahost (ahost." . $to->domain() . "[10.0.0.2]); $datestr");
    $msg->add_header("Received", "from mail." . $from->domain() . " (mail." . $from->domain() . " [192.168.0.1]) by gateway." . $to->domain() . " (gateway." . $to->domain() . " [10.0.0.1]); $datestr");
    $msg->add_header("MIME-Version", "1.0");
    $msg->add_header("Content-Type", "text/plain; charset=\"us-ascii\"");
    $msg->add_header("Content-Transfer-Encoding", "7bit");
    $msg->add_header("Subject", $params->{subject});
    $msg->add_header("From", $from);
    $msg->add_header("Message-ID", "<" . $params->{messageid} . ">");
    $msg->add_header("References", $params->{references})
	if defined $params->{references};
    $msg->add_header("Date", $datestr);
    $msg->add_header("To", $to);
    if (defined($params->{extra_headers})) {
	foreach my $extra_header (@{$params->{extra_headers}}) {
	    $msg->add_header(@{$extra_header});
	}
    }
    $msg->add_header('X-Cassandane-Unique', _generate_unique());
    $msg->set_body("This is a generated test email.  If received, please notify $admin\r\n$extra");

    return $msg;
}


1;
