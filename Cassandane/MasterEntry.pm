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

package Cassandane::MasterEntry;
use strict;
use warnings;
use Cassandane::Util::Log;

my $next_tag = 1;

sub new
{
    my ($class, %params) = @_;

    my $name = delete $params{name};
    if (!defined $name)
    {
	$name = "xx$next_tag";
	$next_tag++;
    }

    my $argv = delete $params{argv};
    die "No argv= parameter"
	unless defined $argv && scalar @$argv;

    my $self =
    {
	name => $name,
	argv => $argv,
    };

    foreach my $a ($class->_otherparams())
    {
	$self->{$a} = delete $params{$a}
	    if defined $params{$a};
    }
    die "Unexpected parameters: " . join(" ", keys %params)
	if scalar %params;

    return bless $self, $class;
}

sub _otherparams
{
    my ($invocant) = @_;
    my $class = ref($invocant) || $invocant;
    my $varname = $class . "::otherparams";
    no strict "refs";
    return @$varname;
}

# Return a hash of key,value pairs which need to go into the line in the
# cyrus master config file.
sub master_params
{
    my ($self) = @_;
    my $params = {};
    foreach my $a ('name', 'argv', $self->_otherparams())
    {
	$params->{$a} = $self->{$a}
	    if defined $self->{$a};
    }
    return $params;
}

package Cassandane::MasterStart;
use base qw(Cassandane::MasterEntry);

our @otherparams;

sub new
{
    return shift->SUPER::new(@_);
}

package Cassandane::MasterEvent;
use base qw(Cassandane::MasterEntry);

our @otherparams = qw(period at);

sub new
{
    return shift->SUPER::new(@_);
}

1;
