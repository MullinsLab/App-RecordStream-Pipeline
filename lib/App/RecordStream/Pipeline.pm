use strict;
use warnings;
use utf8;

package App::RecordStream::Pipeline;
use parent 'Exporter::Tiny';

use App::RecordStream::Pipeline::Operation;
use App::RecordStream::Pipeline::Sink::ArrayRef;
use App::RecordStream::Pipeline::Sink::FileHandle;
use IO::String;
use Types::Standard qw< :types >;
use namespace::clean;

our @EXPORT = qw[ recs sink ];

sub recs {
    my ($name, $args, $next) = @_;
    App::RecordStream::Pipeline::Operation->new(
        name => $name,
        args => $args,
        ($next
            ? (next => $next)
            : ()),
    );
}

sub sink {
    my $output = shift;

    $output = IO::String->new($output)
        if ScalarRef->check($output);

    return App::RecordStream::Pipeline::Sink::FileHandle->new( handle => $output )
        if FileHandle->check($output);

    return App::RecordStream::Pipeline::Sink::ArrayRef->new( records => $output )
        if ArrayRef->check($output);

    # Assume they know what they're doing... maybe a custom object.  The type
    # checking in App::RecordStream::Pipeline::Operation will catch problems.
    return $output;
}

1;
