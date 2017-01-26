use strict;
use warnings;
use utf8;
use 5.010;

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

has name => (
    is       => 'ro',
    isa      => Str,
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

has _operation => (
    is      => 'lazy',
    isa     => InstanceOf['App::RecordStream::Operation'],
);

sub _build__operation {
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

sub run {
    my $self  = shift;
    my $input = shift;
    my $filename;

    # Not all operations want input, usually because they handle
    # reading/generating it themselves.
    if ($self->_operation->wants_input) {
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
                if (not $self->_operation->accept_line($line)) {
                    last;
                }
            }
        }
        # Array of lines or records
        elsif ((ArrayRef[Str] | ArrayRef[HashRef])->check($input)) {
            if (ref $input->[0]) {
                $self->_operation->accept_record( App::RecordStream::Record->new($_) )
                    for @$input;
            } else {
                $self->_operation->accept_line($_)
                    for @$input;
            }
        }
        else {
            die "Unknown input: ", $self->_dump($input);
        }
    }
    $self->_operation->finish;
    return $self->output_sink;
}

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
