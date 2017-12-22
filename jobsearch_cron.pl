#!/usr/bin/perl -w
#
use strict;

# One nice to have that isn't here is a lock file that would cause the
# script to exit without doing anything if  another copy is running.
# The code makes the assumption that only copy is running so there is
# a large potential for run conditions that could cause duplicate entries and
# what not.

use DBI;
use DBD::mysql;
use XML::RSS;
use LWP::Simple;
#use LWP::UserAgent;
use DateTime;
use DateTime::Format::RFC3339;
use DateTime::Format::MySQL;

my $config = '/home/jgilbert/bin/jobsearch/config.pl';
my %config;

&read_config();

my $dsn = "dbi:mysql:database=$config{db_name};" . 
          "host=$config{db_host};port=$config{db_port}";
my $dbh = DBI->connect($dsn, $config{db_user}, $config{db_password},
          {RaiseError => 1});

my $sth = $dbh->prepare("select hostname, path, area, jobtype from Area as a, JobType as jt, Search_Area as sa where sa.area=a.id and sa.jobtype=jt.id group by area, jobtype");
$sth->execute();

my $sth2 = $dbh->prepare("select date from Posting where feed=?" .
                         " order by date desc limit 1");

while (my $ref = $sth->fetchrow_hashref()) {
  my $url = $config{craigslist_url};
  $url =~ s/<hostname>/$$ref{hostname}/;
  $url =~ s/<path>/$$ref{path}/;
 
  $sth2->execute($url);

  my @row_ary = $sth2->fetchrow_array;
  my $latest;
  if ($row_ary[0]) {
    $latest = DateTime::Format::MySQL->parse_datetime($row_ary[0]);
  } else {
    $latest = DateTime::Format::MySQL->parse_datetime('2015-01-01 00:00:00');
  }

  my $rss = new XML::RSS;
  my $xml = get($url);
  $rss->parse($xml);

  my $f = DateTime::Format::RFC3339->new();
  foreach my $item ( @{$rss->{items}}) {
    my $post_dt = $f->parse_datetime($$item{dc}{date});
    next if $post_dt <= $latest;
    $dbh->do("INSERT INTO Posting (title, link, description, date, " . 
             "language, rights, feed, area, jobtype)" . 
             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", undef,
             "$$item{title}",
             "$$item{link}",
             "$$item{description}",
             "$$item{dc}{date}",
             "$$item{dc}{language}",
             "$$item{dc}{rights}",
             "$url",
             "$$ref{area}",
             "$$ref{jobtype}",
             );
  }
}

sub read_config () {

  open(CONFIG,"$config") or die "Couldn't open $config: $!\n";
  while (<CONFIG>) {
    if (/([A-Za-z_]+) = \'([^']+)'/) {
      $config{$1} = $2;
    }
  }
}
