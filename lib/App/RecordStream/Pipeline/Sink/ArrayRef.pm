use strict;
use warnings;
use utf8;

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
