#!/usr/bin/perl
use strict;

# usage:
# pdftotext -raw HKShort.pdf - > pdf.txt
# ~/gitlabs/data-collection/topshorts.ms/getDTC.pl pdf.txt > topshort.csv

open(IN, $ARGV[0] ) or die "cant open file";

# check first 4 lines for structure
my @lns = <IN>;
my $ln;
$ln = $lns[0];
if ($ln !~ /SECURITIES LENDING/) { 
    print STDOUT "unexpected line " + 1 + $ln;
}
$ln = $lns[1];
if ($ln !~ /Hong Kong.* Top Shorts/) { 
    print STDOUT "unexpected line " + 1 + $ln;
}
$ln = $lns[2];
if ($ln !~ /Short Market Value Days To Cover/) {
    print STDOUT "unexpected line " + 1 + $ln;
}
$ln = $lns[3];
if ($ln !~ /Description BBG Sector/) {
    print STDOUT "unexpected line " + 1 + $ln;
}

# grab top 10
for (my $ct = 4; $ct < 14; $ct++) {
    $ln = $lns[$ct];
    chomp($ln);
    if ($ln =~ /^([A-Z\&\-\ ]+) (\d+) HK ([a-zA-Z\&\-\.\ ]+) (\(?[\d.%]+\)?) ([A-Z\&\-\ ]+) (\d+) HK ([a-zA-Z\&\-\.\ ]+) (\d+) ([A-Z\&\-\ ]+) (\d+) HK ([a-zA-Z\&\-\.\ ]+) (\(?[\d.%]+\)?)$/) {
	print STDOUT "$5,$6 HK,$7,$8\n";
    }
}
$ln = $lns[15];
if ($ln !~ /represents the change in shares short/) {
    print STDOUT "unexpected line " + 1 + $ln;
}

