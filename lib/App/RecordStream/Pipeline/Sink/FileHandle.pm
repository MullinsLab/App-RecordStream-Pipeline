use strict;
use warnings;
use utf8;

=encoding UTF-8

=head1 NAME

App::RecordStream::Pipeline::Sink::FileHandle - Write output lines to a file handle

=head1 SYNOPSIS

    my $sink = App::RecordStream::Pipeline::Sink::FileHandle->new( handle => $fh );

=head1 DESCRIPTION

This is an L<App::RecordStream::Stream::Base> subclass which accepts lines of
output and writes them to the given file handle.

It is appropriate as the final link in an L<App::RecordStream::Operation>
chain.

=head1 ATTRIBUTES

=head2 handle

An open file handle suitable for writing to.  Defaults to C<STDOUT>.

=cut

package App::RecordStream::Pipeline::Sink::FileHandle;
use Moo;
extends 'App::RecordStream::Stream::Base';

use Types::Standard qw< :types >;
use namespace::clean;

has handle => (
    is      => 'ro',
    isa     => FileHandle,
    default => sub { \*STDOUT },
);

sub accept_line {
    my ($self, $line) = @_;
    $self->handle->print("$line\n");
}

1;
