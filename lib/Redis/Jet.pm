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
                    build_message build_message_utf8
                    send_message send_message_utf8
                    parse_message parse_message_utf8
                    read_message read_message_utf8
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
        last_error => '',
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
    $self->{sockbuf} = '';
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

sub last_error {
    my $self = shift;
    if ( @_ ) {
        delete $self->{sock};
        delete $self->{fileno};
        $self->{last_error} = shift;
        return;
    }
    return $self->{last_error};
}

sub command {
    my $self = shift;
    return unless @_;
    my $cmds = 1;
    if ( ref $_[0] eq 'ARRAY' ) {
        $cmds = @_;
    }
    my $fileno = $self->{fileno} || fileno($self->connect);
    my $sended = $self->{utf8}
        ? send_message_utf8($fileno, $self->{timeout}, @_)
        : send_message($fileno, $self->{timeout}, @_);
    if ( $sended < 0 ) {
        return $self->last_error('failed to send message: '. (($!) ? "$!" : "timeout"));
    }
    if ( $self->{noreply} ) {
        phantom_read($fileno);
        return ["0 but true"];
    }
    my @res;
    my $res = $self->{utf8}
        ? read_message_utf8($fileno, $self->{timeout}, \@res, $cmds)
        : read_message($fileno, $self->{timeout}, \@res, $cmds);
    if ( $res == -1 ) {
        return $self->last_error('failed to read message: message corruption');
    }
    if ( $res == -2 ) {
        return $self->last_error('failed to read message: '. (($!) ? "$!" : "timeout"));
    }
    return $res[0] if $cmds == 1;
    @res;
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

