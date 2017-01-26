use strict;
use warnings;
use utf8;

package App::RecordStream::Pipeline::Sink::FileHandle;
use Moo;
extends 'App::RecordStream::Stream::Base';

use Types::Standard qw< :types >;
use namespace::clean;

has handle => (
    is      => 'ro',
    isa     => FileHandle,
    default => sub { \*STDIN },
);

sub accept_line {
    my ($self, $line) = @_;
    $self->handle->print("$line\n");
}

1;
