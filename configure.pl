#! /usr/bin/perl -w
#
# Configure Makefile.include by setting the compiler, classpath, and
# compile flags. Either read a previously-created cache file for values
# or ask the user and store those values for next time.
#

$DEBUG = 0;
$INCLUDE_FILE = 'Makefile.include';
$TEMPLATE_FILE = "$INCLUDE_FILE.in";
$CACHE_FILE = 'configure.cache';
$DEFAULT_JIKES_FLAGS = '-g +P +E';

my(%options);

checkForTemplateFile();
%options = readOrAskForValues();
if ($DEBUG) {
    foreach $key (keys(%options)) {
	print "$key => $options{$key}\n";
    }
}
updateIncludeFile(%options) unless $DEBUG;
exit(0);

# Die if we are not in the same directory as $INCLUDE_FILE.
sub checkForTemplateFile {
    if (! -f $TEMPLATE_FILE) {
	die "Error: this script must be run in the same directory as"
	    . " $TEMPLATE_FILE.\n";
    }
}

sub readOrAskForValues {
    return readCache() if -r $CACHE_FILE;

    my(%options) = askUserForValues();
    writeCache(%options);
    return %options;
}

sub readCache {
    my(%options) = ();

    open(CACHE, $CACHE_FILE) || warn "can't read $CACHE_FILE: $!\n";
    while (<CACHE>) {
	chomp();
	m/^(\w+)\s*=\s*(.*)/;
	$options{$1} = $2;
    }
    close(CACHE);
    return %options;
}

sub writeCache {
    my(%options) = @_;

    open(CACHE, ">$CACHE_FILE") || warn "can't write $CACHE_FILE: $!\n";
    foreach $key (keys(%options)) {
	print CACHE "$key = $options{$key}\n";
    }
    close(CACHE);
}

# For each makefile variable we wish to set, guess defaults and ask user
# for values.
sub askUserForValues {
    my($default, %options);

    # CC
    $options{'compile'} = getCompileCommand();

    # JFLAGS
    $default = ($options{'compile'} =~ /jikes$/) ? $DEFAULT_JIKES_FLAGS : '';
    $options{'flags'} =
	getInputWithDefault("Compile flags (except for classpath)", $default);

    # Classpath. Use path to this script as default. Ask user for full path
    # to JGroups, then strip off trailing 'JGroups'.
    $options{'classpath'} = getClasspath();

    return %options;
}

# Look for jikes in path. If not found, look for javac. Return full path
# to whatever we find, or just 'javac' if not found anywhere in path.
sub getCompileCommand {
    my($command);

    $command = whence('jikes');
    $command = whence('javac') unless $command;
    $command = 'javac' unless $command;

    return getInputWithDefault("Compile command", $command);
}

# Return classpath for JGroups (that is, the directory above
# JGroups). Use $PWD as the default because this script can only be run
# in the same directory as the $INCLUDE_FILE. Show "JGroups" to user as
# part of path then strip it off before returning the path.
sub getClasspath {
    my($pwd, $path);

    chomp($pwd = `pwd`);

    my($jgpath) = $pwd;
    $jgpath =~ s!/?jgroups/?$!!;
    $jgpath =~ s!/?org/?$!!;
    $jgpath =~ s!/?src/?$!!;
    $path = getInputWithDefault("Path to JGroups", $jgpath);
##    $path =~ s!/?JGroups/?$!!;

    return $path;
}

# Find specified command in user's path and return full path to command.
# If not found, returns ''.
sub whence {
    my($command) = @_;
    my($dir);

    foreach $dir (split(/:/, $ENV{'PATH'})) {
	return "$dir/$command" if -x "$dir/$command";
    }
    return '';
}

# Given a prompt and a default value, ask the user to either accept the
# default value or enter a new value.
sub getInputWithDefault {
    my($prompt, $default) = @_;
    my($value);

    print "$prompt [$default]: ";
    chomp($value = <STDIN>);
    $value = $default unless $value;
    return $value;
}

# Given hash of options, create $INCLUDE_FILE from $TEMPLATE_FILE.
sub updateIncludeFile {
    my(%options) = @_;

    open(TEMPLATE, $TEMPLATE_FILE) || die "Can't open $TEMPLATE_FILE: $!\n";
    open(INC, ">$INCLUDE_FILE") || die "Can't open temp file $INCLUDE_FILE: $!\n";

    while (<TEMPLATE>) {
	s/\@JG_BASE_DIR\@/$options{'classpath'}/;
	s/\@JFLAGS\@/$options{'flags'}/;
	s/\@JAVAC\@/$options{'compile'}/;
	print INC;
    }

    close(INC);
    close(TEMPLATE);
}
