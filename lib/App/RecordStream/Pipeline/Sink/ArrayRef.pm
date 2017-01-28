use strict;
use warnings;
use utf8;

=encoding UTF-8

=head1 NAME

App::RecordStream::Pipeline::Sink::ArrayRef - Collect an array of records

=head1 SYNOPSIS

    my $sink = App::RecordStream::Pipeline::Sink::ArrayRef->new;
    ...
    $sink->records;

    my $sink = App::RecordStream::Pipeline::Sink::ArrayRef->new( records => \my @records );
    ...
    say "Got ", scalar @records, " records";

=head1 DESCRIPTION

This is an L<App::RecordStream::Stream::Base> subclass which accepts
L<App::RecordStream::Record> objects, converts them to a normal (unblessed)
hashref, and stores them in the L</records> attribute.

It is appropriate as the final link in an L<App::RecordStream::Operation>
chain.

=head1 ATTRIBUTES

=head2 records

An arrayref of hashrefs.

=cut

package App::RecordStream::Pipeline::Sink::ArrayRef;
use Moo;
extends 'App::RecordStream::Stream::Base';

use Types::Standard qw< :types >;
use namespace::clean;

has records => (
    is      => 'ro',
    isa     => ArrayRef,
    default => sub { [] },
);

sub accept_record {
    my ($self, $record) = @_;
    push @{ $self->records }, $record->as_hashref;
}

1;
