package IPC::ConcurrencyLimit::Lock::ZooKeeper;
use 5.008001;
use strict;
use warnings;
# ABSTRACT: Locking via ZooKeeper

use Carp qw(croak);
use Net::ZooKeeper::Lock;
use Net::ZooKeeper;

use parent 'IPC::ConcurrencyLimit::Lock';

=method_public new

Constructor, takes named parameters, see C<Attributes>.

=cut

=attribute_public max_procs

The maximum number of concurrent instances of the same
lock.

=attribute_public path

The path within ZooKeeper to use as prefix for this lock.
Format: C</path/to/my/lock>

=attribute_public hostname

Hostname of the ZooKeeper instance.

=attribute_public port

Port number of the ZooKeeper instance (defaults to 2181).

=cut

our %ZooKeepers;
our %ZooKeeperLocks;

sub hostname { $_[0]->{hostname} }
sub port { $_[0]->{port} }
sub path { $_[0]->{path} }
sub max_procs { $_[0]->{max_procs} }

sub new {
  my $class = shift;
  my $opt = shift;

  my $max_procs = $opt->{max_procs}
    or croak("Need a 'max_procs' parameter");
  my $path = $opt->{path}
    or croak("Need a 'path' parameter");
  my $hostname = $opt->{hostname}
    or croak("Need a 'hostname' parameter");
  my $port = $opt->{port} || 2181;

  my $self = bless {
    max_procs => $max_procs,
    path      => $path,
    hostname  => $hostname,
    port      => $port,
    zk_lock   => undef,
  } => $class;

  $self->_get_lock() or return undef;

  return $self;
}

sub _get_lock {
  my $self = shift;

  my $prefix = $self->{path};
  my $hostname = $self->{hostname};
  my $port = $self->{port};
  my $hp = "$hostname:$port";
  my $zkh = $ZooKeepers{$hp} ||= Net::ZooKeeper->new($hp);
  $ZooKeeperLocks{$hp}++;
  for my $worker (1 .. $self->{max_procs}) {
    my $lock = Net::ZooKeeper::Lock->new(
      zkh => $zkh,
      create_prefix => 1,
      lock_prefix => $prefix,
      lock_name => "lock$worker",
      blocking => 0,
    );
    
    if ($lock) {
      $self->{zk_lock} = $lock;
      $self->{id} = $worker;
      last;
    }
  }

  if (not $self->{zk_lock}) {
    $ZooKeeperLocks{$hp}--;
    if ($ZooKeeperLocks{$hp} <= 0) {
      delete $ZooKeeperLocks{$hp};
      delete $ZooKeepers{$hp};
    }
    return undef;
  }
  return 1;
}


# Normally needs implementing to release the lock,
# but in this case, we just hold on to the ZK object that does it
# for us when destroyed itself.
# Thus, it will be released as soon as this object is freed.
sub DESTROY {
  my $self = shift;
  if (defined $self->{id}) {
    my $hp = $self->{hostname} . ":" . $self->{port};
    $ZooKeeperLocks{$hp}--;
    # reset zookeeper connection, too
    if ($ZooKeeperLocks{$hp} <= 0) {
      delete $ZooKeeperLocks{$hp};
      delete $self->{zk_lock};
      delete $ZooKeepers{$hp};
    }
  }
}

1;

__END__


=head1 NAME

IPC::ConcurrencyLimit::Lock::ZooKeeper - Locking via ZooKeeper

=head1 SYNOPSIS

  use IPC::ConcurrencyLimit;

=head1 DESCRIPTION

This locking strategy uses L<Net::ZooKeeper::Lock> to implement
locking via ZooKeeper.

B<Beware:> This is alpha-quality software. Before relying on it
in production, please get in touch with the author to check for
potential updates, new gotchas, and stability. More specifically,
there is a poorly understood issue with locks involving many
ZooKeeper sequence znodes and the quality of this module cannot be
higher than that of the underlying lock implementation in
L<Net::ZooKeeper::Lock>.

Inherits from L<IPC::ConcurrencyLimit::Lock>.

=head1 SEE ALSO

=for :list
* L<IPC::ConcurrencyLimit>
* L<IPC::ConcurrencyLimit::Lock>
* L<Net::ZooKeeper>
* L<Net::ZooKeeper::Lock>

=cut

