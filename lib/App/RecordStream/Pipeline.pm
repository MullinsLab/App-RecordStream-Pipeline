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

=head1 NAME

App::RecordStream::Pipeline

=head1 SYNOPSIS

    use App::RecordStream::Pipeline;
    my $pipeline = recs->fromcsv(qw[ --header --strict ], "data.csv")
                       ->grep( sub { $_->{age} >= 21 } )
                       ->sort( qw[ --key income=-numeric ])
                       ->totable;

    print $pipeline->run;

=head1 DESCRIPTION

App::RecordStream::Pipeline provides a programmatic interface for using
L<App::RecordStream> operations in a Perl script. Pipelines are built up with
chained method calls, and the resulting objects may then be composed with each
other.

=head1 EXPORTS

=head2 recs

A shortcut for App::RecordStream::Pipeline->new, providing a convenient
way to start a pipeline

=cut

our @EXPORT = qw< recs >;

sub recs {
    App::RecordStream::Pipeline->new
};

=head1 METHODS

=head2 recs operations

An instance of App::RecordStream::Pipeline has methods corresponding to all
L<App::RecordStream::Operation> packages found in C<@INC>. Each takes the same
options and arguments as that operation takes in the command line version of C<recs>.

=cut

my @operations = map { s/^App::RecordStream::Operation:://; $_ }
                 App::RecordStream->operation_packages;
for my $op (@operations) {
    no strict "refs";
    *{"$op"} = sub { my $self = shift; $self->_chain_operation($op, @_); };
}
has pipeline => (
    is      => 'ro',
    isa     => ArrayRef,
    default => sub { [] },
);

=head2 then

Given an L<App::RecordStream::Pipeline>, returns a pipeline where the
argument's operations follow the caller's.

=cut

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

=head2 run

Run the pipeline operations, passing the output of each to the input of the next.

=head3 parameters

=over 4

=item input

If a filehandle, its contents will be read as lines and provided as input to
the pipeline's first operation. If an array reference of strings, ditto. If an
array reference of hashrefs, each entry will be passed as a record to the
pipeline's first operation, without parsing.

Not all operations look for input; the "from*" operations generally take a list
of filenames as arguments, and read from STDIN otherwise. Operations that
transform records into different records will generally accept input given in
the L</input> parameter to L</run> parameter.

=item output

A filehandle to which output will be streamed as text, whether as JSON records
or formatted output from a "to*" operation. If L</output> is provided, it will
be used as the return value of L</run>. Otherwise, L</run> returns a string
(for "to*" operations that generate output) or a list of hashrefs (for
operations that return records).

=back
=cut

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
