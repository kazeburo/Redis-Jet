package Redis::Jet;

use 5.008005;
use strict;
use warnings;
use POSIX qw(EINTR EAGAIN EWOULDBLOCK :sys_wait_h);
use IO::Socket qw(:crlf IPPROTO_TCP TCP_NODELAY);
use IO::Socket::INET;
use IO::Select;
use Time::HiRes qw/time/;
use base qw/Exporter/;

our @EXPORT_OK = qw/
                    parse_message parse_message_utf8
                /;

our $VERSION = "0.01";

use XSLoader;
XSLoader::load(__PACKAGE__, $VERSION);

sub new {
    my $class = shift;
    my %args = ref $_ ? %{$_[0]} : @_;
    %args = (
        server => '127.0.0.1:6379',
        timeout => 10,
        utf8 => 0,
        noreply => 0,
        %args,
    );
    my $server = shift;
    my $self = bless \%args, $class;
    $self;
}

sub connect {
    my $self = shift;
    return $self->{sock} if $self->{sock};
    my $socket = IO::Socket::INET->new(
        PeerAddr => $self->{server},
        Timeout => $self->{timeout},
    ) or return;
    $socket->setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
        or die "setsockopt(TCP_NODELAY) failed:$!";
    $socket->blocking(0) or die $!;
    $self->{sock} = $socket;
    $self->{fileno} = fileno($socket);
    $socket;
}

sub res_error {
    my $self = shift;
    delete $self->{sock};
    delete $self->{fileno};
    return [undef,$_[0]];
}

sub command {
    my $self = shift;
    return unless @_;
    if ( !$self->{fileno} ) {
        $self->connect;
        $self->{fileno} or return $self->res_error('cannot connect to redis server: '. (($!) ? "$!" : "timeout"));
    }
    run_command($self, @_);
}


1;
__END__

=encoding utf-8

=head1 NAME

Redis::Jet - It's new $module

=head1 SYNOPSIS

    use Redis::Jet;

=head1 DESCRIPTION

Redis::Jet is ...

=head1 LICENSE

Copyright (C) Masahiro Nagano.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Masahiro Nagano E<lt>kazeburo@gmail.comE<gt>

=cut

