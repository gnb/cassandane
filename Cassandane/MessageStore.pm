#!/usr/bin/perl
#
#  Copyright (c) 2011-2012 Opera Software Australia Pty. Ltd.  All rights
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

package Cassandane::MessageStore;
use strict;
use warnings;
use Cassandane::Util::Log;
use overload qw("") => \&as_string;

sub new
{
    my ($class, %params) = @_;
    my $self = {
	verbose => delete $params{verbose} || 0,
    };
    die "Unknown parameters: " . join(' ', keys %params)
	if scalar %params;
    return bless $self, $class;
}

sub connect
{
    my ($self) = @_;
    die "Unimplemented in base class " . __PACKAGE__;
}

sub disconnect
{
    my ($self) = @_;
    die "Unimplemented in base class " . __PACKAGE__;
}

sub write_begin
{
    my ($self) = @_;
    die "Unimplemented in base class " . __PACKAGE__;
}

sub write_message
{
    my ($self, $msg, %opts) = @_;
    die "Unimplemented in base class " . __PACKAGE__;
}

sub write_end
{
    my ($self) = @_;
    die "Unimplemented in base class " . __PACKAGE__;
}

sub read_begin
{
    my ($self) = @_;
    die "Unimplemented in base class " . __PACKAGE__;
}

sub read_message
{
    my ($self) = @_;
    die "Unimplemented in base class " . __PACKAGE__;
}

sub read_end
{
    my ($self) = @_;
    die "Unimplemented in base class " . __PACKAGE__;
}

sub get_client
{
    my ($self) = @_;
    die "Unimplemented in base class " . __PACKAGE__;
}

sub as_string
{
    my ($self) = @_;
    return "unknown";
}

1;
