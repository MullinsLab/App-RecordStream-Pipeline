use strict;
use warnings;
use utf8;

package App::RecordStream::Pipeline;

use Moo;
extends 'Exporter::Tiny';
use App::RecordStream;
use App::RecordStream::Pipeline::Operation;
use App::RecordStream::Pipeline::Sink::ArrayRef;
use App::RecordStream::Pipeline::Sink::FileHandle;
use List::Util qw< reduce >;
use Types::Standard qw< :types >;
use namespace::clean;

our @EXPORT = qw< recs >;

my @operations = map { s/^App::RecordStream::Operation:://; $_ }
                 App::RecordStream->operation_packages;
for my $op (@operations) {
    no strict "refs";
    *{"$op"} = sub { my $self = shift; $self->_chain_operation($op, @_); };
}

sub recs {
    App::RecordStream::Pipeline->new
};

has pipeline => (
    is      => 'ro',
    isa     => ArrayRef,
    default => sub { [] },
);

sub then {
    my $self = shift;
    my $next = shift;
    return App::RecordStream::Pipeline->new(
        pipeline => [ @{$next->pipeline}, @{$self->pipeline} ]
    );
}

sub _chain_operation {
    my $self = shift;
    my $name = shift;
    my $op = { name => $name, args => \@_ };
    App::RecordStream::Pipeline->new(
        pipeline => [ $op, @{$self->pipeline} ]
    );
}

sub run {
    my $self = shift;
    my %params = @_;

    # Set up a sink and a callback to return the desired value based on
    # the end of the pipeline.
    my $sink;
    my $return;
    my $final = $self->pipeline->[0];

    if ($params{output} && FileHandle->check($params{output})) {
        # If given an output filehandle, write to it and return the filehandle,
        # no matter what's in the pipeline
        $sink = App::RecordStream::Pipeline::Sink::FileHandle->new( handle => $params{output} );
        $return = sub { $params{output} };
    } elsif ($final->{name} =~ /^to(?!pn$)/) {
        # If the pipeline ends with a "to$format" operation, we're probably going
        # to get a string, so accumulate the results as a string
        my $result;
        open my $io, ">", \$result or die "Unable to open handle: $!";
        $sink = App::RecordStream::Pipeline::Sink::FileHandle->new( handle => $io );
        $return = sub {
            close $io or die "Unable to close handle: $!";
            $result
        };
    } else {
        # Otherwise, there's some intermediate pipeline step at the end, and we'll
        # just return a list of hashrefs (i.e. records).
        my @result;
        $sink = App::RecordStream::Pipeline::Sink::ArrayRef->new( records => \@result );
        $return = sub { @result };
    }

    # Build up the pipeline operations, ending with the sink we chose.
    my $compiled = reduce {
        App::RecordStream::Pipeline::Operation->new(
            name => $b->{name},
            args => $b->{args},
            ($a
                ? (next => $a)
                : ()),
        );
    } $sink, @{$self->pipeline};

    # Pass along any input parameter to the initial operation.
    $compiled->run($params{input} ? $params{input} : ());
    $return->();
}

1;
