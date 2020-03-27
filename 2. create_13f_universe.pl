###########################################
### 2. create_13f_universe.pl           ###
### Author: Morad Elsaify               ###
### Date created: 01/29/20              ###
### Date modified: 03/26/20             ###
###########################################

###########################################################################################################
### This file creates a file for each 13F-HR/13F-HR/A/13F-NT/13F-NT/A reporting institution. Each       ###
### master file (downloaded using get_master_files.pl) is iterated through and the rows containing a    ###
### 13F-HR report are appended to the file with the appropriate CIK number. Each file is in the         ###
### subdirectory 'All_13F' with each reporting CIK being given its own file with that name.             ###
###########################################################################################################

use Cwd;
chdir('/hpc/group/fuqua/mie4/EDGAR Parsing/Data/Master Files') or die "cannot change: $!\n";

# initialize directories
$commandunix = "mkdir " . 'All_13F' . "\n"; 
system($commandunix); #creates new directory
$commandunix = "mkdir " . 'Only_13F_HR' . "\n"; 
system($commandunix); #creates new directory

for($year = 1993; $year < 2021; $year = $year+1) {
    for($i = 1; $i < 5; $i = $i+1) {
        # load each quarterly file 
        $filea = $year . 'QTR' . $i . 'master';
        open(MYINFILE, $filea); # assumes master files are in root
        @linesgn = <MYINFILE>;
        close(MYINFILE);

        # get number of aligns, format directory  name
        $sizegn = @linesgn;
        
        for($j = 1; $j < $sizegn; $j++) {
            if($linesgn[$j] =~ m/txt/) {
                @arraydata = split(/\|/, $linesgn[$j]); # gets the data from each line
                $num = sprintf("%07d", $arraydata[0]); # formats CIK number
                $fileall = ">>" . 'All_13F' . "/" . $num . ".dat"; # The ">>" appends
                $filesub = ">>" . 'Only_13F_HR' . "/" . $num . ".dat"; # The ">>" appends

                # save file as csv if any type of 13F report
                if($arraydata[2] eq '13F-HR' or $arraydata[2] eq '13F-HR/A' or 
                    $arraydata[2] eq '13F-NT' or $arraydata[2] eq '13F-NT/A') {
                   open(MYOUTFILE, $fileall); # opens file
                    $datagn=$linesgn[$j]; 
                    $datagn =~ s/,/;/g;
                    $datagn =~ s/\|/,/g;
                    print MYOUTFILE $datagn; 
                    close(MYOUTFILE);
                }
                # save file as CSV if 13F-HR or 13F-HR/A
                if($arraydata[2] eq '13F-HR' or $arraydata[2] eq '13F-HR/A') {
                   open(MYOUTFILE, $filesub); # opens file
                    $datagn=$linesgn[$j]; 
                    $datagn =~ s/,/;/g;
                    $datagn =~ s/\|/,/g;
                    print MYOUTFILE $datagn; 
                    close(MYOUTFILE);
                }
            }
        }
    }
}

