use strict;
use warnings;
use utf8;
use 5.010;

=head1 NAME

App::RecordStream::Pipeline::Operation - One link in a chain of operations

=head1 DESCRIPTION

This class represents one L<App::RecordStream::Operation> in a chain (linked
list) of operations.  The actual operation objects are not instantiated until
the first operation in the chain is run.

Unless you're doing something unusual, you'll probably want to use
L<App::RecordStream::Pipeline> instead of using this class directly.

=cut

package App::RecordStream::Pipeline::Operation;
use Moo;
use App::RecordStream::Operation;
use App::RecordStream::Pipeline::Sink::ArrayRef;
use B::Deparse;
use Data::Dumper qw< Dumper >;
use Scalar::Util qw< refaddr blessed >;
use Types::Standard qw< :types >;
use namespace::clean;

our %__SUBS;

=head1 ATTRIBUTES

=head2 name

The operation name, such as C<fromcsv> or C<grep>.  This will be prefixed with
C<App::RecordStream::Operation::> to construct a full package name which is
then loaded.

Required.

=head2 args

An arrayref of arguments to pass to the operation constructor.  These arguments
are equivalent to the operation's command line arguments.  You can run
C<< recs help <operation-name> >> to list them.

In places where you'd normally specify a Perl snippet, you can instead provide
a coderef to call.  See L</SNIPPETS AND CODEREFS> for more details.

=head2 next

The next operation or sink in the chain, to which this operation will pass
output.

This must either be another L<App::RecordStream::Pipeline::Operation> or a
subclass of L<App::RecordStream::Stream::Base>.

Required.  Defaults to an L<App::RecordStream::Pipeline::Sink::ArrayRef> if not
provided.

=cut

my $OperationName = Str & sub {
    App::RecordStream::Operation::is_recs_operation("recs-$_[0]")
};

has name => (
    is       => 'ro',
    isa      => $OperationName,
    required => 1,
);

has args => (
    is      => 'ro',
    isa     => ArrayRef[ Str|CodeRef ],
    default => sub { [] },
);

has next => (
    is      => 'ro',
    isa     =>   InstanceOf['App::RecordStream::Pipeline::Operation']
               | InstanceOf['App::RecordStream::Stream::Base'],
    default => sub { App::RecordStream::Pipeline::Sink::ArrayRef->new },
);


=head1 METHODS

=head2 run

Constructs and runs the chain of operations.  It generally only makes sense to
call this on the first operation in a chain.

If the operation doesn't handle its own input (like C<fromcsv>), then an input
value is required as the sole argument.

The input may be an open file handle, an arrayref of strings, or an arrayref of
hashrefs (records).  Each line or hashref is fed into the operation.

Returns the chain's sink via L</output_sink>.

=cut

sub run {
    my $self      = shift;
    my $input     = shift;
    my $operation = $self->_operation;
    my $filename;

    # Not all operations want input, usually because they handle
    # reading/generating it themselves.
    if ($operation->wants_input) {
        die "Input required for ", $self->name, "\n"
            unless $input;

        # STDIN, ARGV, DATA, or some other handle
        if (FileHandle->check($input)) {
            my %special = (
                \*ARGV  => \$ARGV,      # XXX TODO: This won't work because FileHandle->check(\*ARGV)
                                        # is false!  (Due to Scalar::Util::openhandle())
                \*DATA  => \'__DATA__', # XXX TODO: This won't work because \*DATA isn't global!
                \*STDIN => \'stdin',
            );

            my $filename = $special{$input}
                ? $special{$input}
                : \sprintf "fd#%d", fileno($input);

            while (my $line = <$input>) {
                chomp $line;
                App::RecordStream::Operation::set_current_filename( $$filename );
                if (not $operation->accept_line($line)) {
                    last;
                }
            }
        }
        # Array of lines or records
        elsif ((ArrayRef[Str] | ArrayRef[HashRef])->check($input)) {
            if (ref $input->[0]) {
                $operation->accept_record( App::RecordStream::Record->new($_) )
                    for @$input;
            } else {
                $operation->accept_line($_)
                    for @$input;
            }
        }
        else {
            die "Unknown input: ", $self->_dump($input);
        }
    }
    $operation->finish;
    return $self->output_sink;
}

sub _operation {
    my $self = shift;
    my $name = $self->name;
    my $args = $self->_processed_args;
    my $next = $self->next->isa("App::RecordStream::Pipeline::Operation")
        ? $self->next->_operation
        : $self->next;

    return App::RecordStream::Operation::create_operation("recs-$name", $args, $next),
}

sub _processed_args {
    my $self = shift;
    my $args = $self->args;
    return [ map { $self->_process_arg($_) } @$args ];
}

sub _process_arg {
    my $self = shift;
    my $arg  = shift;

    if (ref $arg eq 'CODE') {
        # Stash the coderef in our shared registry and replace it in the
        # argument list with a snippet that calls the original sub via the
        # shared registry.  Deparse the coderef and inline as a comment for
        # debugging.
        my $key = refaddr($arg);
        my $comment = $self->_coderef_to_comment($arg);

        $__SUBS{ $key } = $arg;

        $arg = sprintf <<'        CODE', $comment, __PACKAGE__, $key;

            %s
            { local $_ = $r;
              $%s::__SUBS{ q{%s} }->($r) }
        CODE
        $arg =~ s/^ {12}//mg;
    }

    return $arg;
}

sub _coderef_to_comment {
    state $deparse = do {
        # Output original line numbers for easier debugging
        my $d = B::Deparse->new('-l');

        # Silence most common pragma from being output in text
        $d->ambient_pragmas(
            strict   => 'all',
            warnings => 'all',
            re       => 'all',
            integer  => 0,
            bytes    => 0,
            utf8     => 0,
        );

        $d;
    };

    my $self = shift;
    my $sub  = shift;
    my $text = $deparse->coderef2text($sub);
       $text =~ s/^/# /mg;

    return $text;
}


=head2 output_sink

Walks the chain of operations by calling L</next> until a stream sink is found.

Returns the sink object, usually an
L<App::RecordStream::Pipeline::Sink::ArrayRef> or
L<App::RecordStream::Pipeline::Sink::FileHandle> (but it may be any
L<App::RecordStream::Stream::Base> subclass).

=cut

sub output_sink {
    my $self = shift;
    my $next = $self->next;
    return $next->output_sink if $next->isa("App::RecordStream::Pipeline::Operation");
    return $next;
}

sub _dump {
    my $self = shift;
    return Data::Dumper->new([@_])->Terse(1)->Dump;
}

1;
