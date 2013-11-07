### main pod documentation begin ###################

=head1 NAME

TSM_DB::DB_Object

=head1 SYNOPSIS

=head1 DESCRIPTION

Based (loosely) on Object by Mark Rajcok and earlier work by rpillar this 
is a module that provides a number of 'generic' db access methods in order 
to make it easier for retrieve / update / insert etc - data into a db table 
without having to explictly write any 'sql'.

Updated to make use of 'Moose'.

=head1 USAGE

It should never be necessary to access this module directly - all access will be 
via the 'inheriting' DB_Table object - see for notes on usage.

=head1 BUGS

None we're aware of...

=head1 SUPPORT

=head1 AUTHOR

	rpillar
    http://www.developontheweb.co.uk

=head1 COPYRIGHT

This program is free software licensed under the...


=head1 SEE ALSO

perl(1).

=cut

#################### main pod documentation end ###################

package DB_Object;

use 5.10.1;
use Moose;
use namespace::autoclean;

use warnings;
use Carp;

use SQL::Abstract;
use Data::Dumper;

# -------------------------------------------------------------------------------------
#  attributes
# -------------------------------------------------------------------------------------

has 'dbh'     => ( is => 'ro', );
has 'data'    => ( is => 'rw', isa => 'HashRef[Any]', );
has 'results' => ( is => 'rw', isa => 'HashRef[Any]', );

# -------------------------------------------------------------------------------------
#  init - create 'data' structure as required ....
# -------------------------------------------------------------------------------------
sub init {
	my $self = shift;
	
	# initialise the tables data structure.
	foreach my $field ( @{$self->{fields}} ) {
		$self->{data}->{$field} = undef;
	}
	$self->{sa} = SQL::Abstract->new;
}

# -------------------------------------------------------------------------------------
#  commands
# -------------------------------------------------------------------------------------

#################### subroutine header start ###################

=head2 delete

 Usage     : $supplier->delete( $dbh, $where );
 Purpose   : deletes data items from a db table. 
 Returns   : nothing
 Argument  : a database handle - 'dbh' and an appropriate 'where' clause :-
 
             my $where = { category => 'Stock', option_name => 'last_short_code' };

 Comment   : none

=cut

#################### subroutine header end ####################

sub delete {
    my ( $self, $where ) = @_;

    my ( $sql, @bind ) = $self->{sa}->delete( $self->{table}, $where );
    my $sth = $self->{dbh}->prepare($sql) || die;
    $sth->execute(@bind) || die;

    return 1;
}

# -------------------------------------------------------------------------------------

#################### subroutine header start ###################

=head2 insert

 Usage     : $supplier->insert( \%data, );
 Purpose   : performs an insert into the specified db / table. 
 Returns   : '0' - success.
 Argument  : a hash_ref holding the name / value pairs of the fields that 
             are to be inserted :-
             
             my %data = { name => 'Fred', status => 'alive' };
              
 Comment   : none

=cut

#################### subroutine header end ####################

sub insert {
    my ( $self, $field_vals ) = @_;

    my ( $sql, @bind ) = $self->{sa}->insert( $self->table(), $field_vals );
    my $sth = $self->{dbh}->prepare($sql) || return "prepare : insert failed - table $self->{table} : $DBI::errstr\n\n";
    $sth->execute(@bind) || return "execute : insert failed - table $self->{table} : $DBI::errstr\n\n";

    return 0;
}

# -------------------------------------------------------------------------------------

#################### subroutine header start ###################

=head2 last_insert_id

 Usage     : my $last_id = $supplier->last_insert_id( $dbh, $table );
 Purpose   : retrieves the id of the most recently 'inserted' record. 
 Returns   : the 'value' of the id - success / '0' - failed.
 Argument  : none
 Comment   : none

=cut

#################### subroutine header end ####################

sub last_insert_id {
    my ( $self ) = @_;

    my $last_id = $self->{dbh}->last_insert_id( undef, undef, $self->table(), undef );
    return $last_id;
}

# -------------------------------------------------------------------------------------

#################### subroutine header start ###################

=head2 load

 Usage     : $supplier->load( $dbh, [qw(id supp_code)], $where );
 Purpose   : retrieves data - places it in the 'objects' data / results attributes. 
 Returns   : nothing
 Argument  : an array_ref holding the names of the requested fields
 Comment   : the 'where' and 'order' arguments are optional. The first resultset item
             is placed in the 'data' attribute (eg. $supplier->{data}), the 'results'
             attribute holds 'all' data items that have been retrieved - the 'next_rec'
             method should be used to access the 'next' data item.   
             Note :- this method performs a 'fetchall' - all 'data'. Hence this method
             should only be used when the amount of data being retrieved is known to be
             limited - otherwise use method 'load_each'        

=cut

#################### subroutine header end ####################

sub load {
    my ( $self, $fields_ref, $where, $order ) = @_;

    my ( $sql, @bind ) = $self->{sa}->select( $self->{table}, $fields_ref, $where, $order );
    my $sth = $self->{dbh}->prepare($sql)
        || return "prepare - load failed : $DBI::errstr\n\n";
    $sth->execute(@bind)
        || return "execute - load failed : $DBI::errstr\n\n";
    my $results = $sth->fetchall_arrayref();

    # populate object with returned data - into the results hash
    $self->{results} = ();
    my $hash_key = 1;
    foreach my $result ( @{$results} ) {
        %{ $self->{results}->{$hash_key} } = %{ $self->{data} };
        my $field_key = 0;
        foreach ( @{$fields_ref} ) {
            $self->{results}->{$hash_key}->{$_} = $result->[$field_key];
            $field_key++;
        }
        $hash_key++;
    }

    # set the data hash so that it contains record '1'
    $hash_key = 1;
    foreach my $field ( @{$fields_ref} ) {
        $self->{data}->{$field} = $self->{results}->{$hash_key}->{$field};
    }
    $self->{rec_pointer} = 1;

    return 0;
}

# -------------------------------------------------------------------------------------

#################### subroutine header start ###################

=head2 load_as_aggregate

 Usage     : $stockitem->load_as_aggregate('max(short_code), count(*)', );
 Purpose   : finds the 'aggregate' values that have been requested. 
 Returns   : nothing
 Argument  : a scaler holding the requested aggregates and a where clause. 
 Comment   : returned values placed in $self->{data}->{aggregates} as an array_ref.

=cut

#################### subroutine header end ####################

sub load_as_aggregate {
    my ( $self, $field, $where ) = @_;

    my ( $sql, @bind ) = $self->{sa}->select( $self->{table}, $field, $where );
    my $sth = $self->{dbh}->prepare($sql) || die "prepare - load failed : $DBI::errstr\n\n";
    $sth->execute(@bind) || die "execute - load failed : $DBI::errstr\n\n";
    my @results = $sth->fetchrow_array();
    $self->{data}->{aggregates} = \@results;

    return 1;
}

# -------------------------------------------------------------------------------------

#################### subroutine header start ###################

=head2 load_as_distinct

 Usage     : $supplier->load_as_distinct( [qw(supp_code)], $where, $sort_flag );
 Purpose   : Creates an 'array' that will contain a set of 'distinct' values for
             a specified field. 
 Returns   : nothing - sets a data array as part of 'self' - referenced using a 
             literal of $field->[0] . '_data'
 Argument  : an array_ref holding the name of the requested field, a 'where' clause 
             and a 'sort' flag ('where' and 'sort' are optional).
 Comment   : the 'standard' where clause is 'IS NOT NULL' - this will get 'all' values
             if no 'where' is specified. The 'sort_flag', if supplied, should be either
             'C' - a character sort or 'N' - a numeric sort.

=cut

#################### subroutine header end ####################

sub load_as_distinct {
    my ( $self, $field, $where, $sort_flag ) = @_;

    unless ($where) {
        $where = { $field->[0] => { '!=', undef }, };
    }

    my ( $sql, @bind ) = $self->{sa}->select( $self->{table}, $field, $where );
    my $sth = $self->{dbh}->prepare($sql) || die "prepare - load failed : $DBI::errstr\n\n";
    $sth->execute(@bind) || die "execute - load failed : $DBI::errstr\n\n";
    my $results = $sth->fetchall_arrayref();

    my $data_hash;
    foreach ( @{$results} ) {
        my $data_item = $_->[0];    # get data value ...
        my $data_key  = $_->[0];
        $data_item =~ s/\s+$//g;      # remove spaces from the end
        $data_key  =~ s/\s+$//g;
        $data_key  =~ tr/a-z/A-Z/;    # convert to uppercase
        $data_hash->{$data_key} = $data_item;
    }
    my @data_array = values %{$data_hash};
    my @sorted_array;
    given ($sort_flag) {
        when ('C') {
            @sorted_array = sort { $a cmp $b } @data_array;
            $self->{ $field->[0] . '_data' } = \@sorted_array;
        }
        when ('N') {
            @sorted_array = sort { $a <=> $b } @data_array;
            $self->{ $field->[0] . '_data' } = \@sorted_array;
        }
        default {
            $self->{ $field->[0] . '_data' } = \@data_array;
        }
    }

    return 1;
}

#################### subroutine header start ###################

=head2 load_each

 Usage     : $supplier->load_each( [qw(id supp_code)], $where );
 Purpose   : retrieves data - places it in the 'objects' data - one at a time 
 Returns   : returns '1' (available data) or '0' (no more data)
 Argument  : an array_ref holding the names of the requested fields, a 'where' clause 
             and an optional 'order_by'.
 Comment   : the 'where' and 'order' arguments are optional. Only the first resultset
             item is loaded into the object. The method needs to be called for each
             row - could be used as part of 'while' loop.         

=cut

#################### subroutine header end ####################

sub load_each {
    my ( $self, $fields_ref, $where, $order ) = @_;

    my $results;

    # only perform the prepare and execute once - otherwise just get the data ...
    unless ( $self->{query} ) {
        my ( $sql, @bind ) = $self->{sa}->select( $self->{table}, $fields_ref, $where, $order );
        my $sth = $self->{dbh}->prepare($sql)
            || return "prepare - load_each failed : $DBI::errstr\n\n";
        $sth->execute(@bind)
            || return "execute - load_each failed : $DBI::errstr\n\n";
        $results = $sth->fetchrow_hashref();
        grep { $self->{data}->{$_} = $results->{$_} } @{$fields_ref};
        $self->{query} = $sth;
        return 1;
    }
    else {
        if ( $results = $self->{query}->fetchrow_hashref() ) {
            grep { $self->{data}->{$_} = $results->{$_} } @{$fields_ref};
            return 1;
        }
        else {
            $self->{query} = undef;
            return 0;
        }
    }
    return 0;
}

# -------------------------------------------------------------------------------------

#################### subroutine header start ###################

=head2 next_rec

 Usage     : $supplier->next_rec( $fields_ref );
 Purpose   : iterate thru the 'results' - get the specified fields for the
             next resultset entry. 
 Returns   : nothing
 Argument  : an 'array_ref' holding the field names that have been requested :-
             
             my $fields_ref = [ qw(name address) ];
              
 Comment   : none

=cut

#################### subroutine header end ####################

sub next_rec {
    my $self       = shift;
    my $fields_ref = shift;

    $self->{rec_pointer}++;
    if ( !$self->{results}->{ $self->{rec_pointer} } ) {
        return 0;
    }
    foreach my $field ( @{$fields_ref} ) {
        $self->{data}->{$field} = $self->{results}->{ $self->{rec_pointer} }->{$field};
    }
    return 1;
}

# -------------------------------------------------------------------------------------

#################### subroutine header start ###################

=head2 update

 Usage     : $supplier->update( \%data, $where );
 Purpose   : performs a db update for '$self->{table}' based on the specified
             'where' clause. 
 Returns   : '0' - success.
 Argument  : a hash_ref holding the name / value pairs of the fields that are to 
             be updated and an optional 'where' clause (a hash_ref holding field 
             names / values) :-
             
             my %data = { name => 'Fred', status => 'alive' };
             my $where = { category => 'Stock', option_name => 'last_short_code' };
              
 Comment   : none

=cut

#################### subroutine header end ####################

sub update {
    my ( $self, $field_vals, $where ) = @_;

    my ( $sql, @bind ) = $self->{sa}->update( $self->table(), $field_vals, $where );
    my $sth = $self->{dbh}->prepare($sql) || return "prepare : update failed - table $self->{table} : $DBI::errstr\n\n";
    $sth->execute(@bind) || return "execute : update failed - table $self->{table} : $DBI::errstr\n\n";

    return 0;
}

# -------------------------------------------------------------------------------------

__PACKAGE__->meta->make_immutable;

1;
