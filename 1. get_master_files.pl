###########################################
### get_master_files.R                  ###
### Author: Morad Elsaify               ###
### Date created: 02/06/20              ###
### Date modified: 03/26/20             ###
###########################################

###########################################################################################################
### This file downloads the master files from EDGAR. Master files contain a list of all filings by SEC  ###
### reporting institutions and list details of filing CIK, name, form type, and address. Perl code to   ###
### download master files copied almost verbatim from the LWP::UserAgent documentation                  ###
### (https://metacpan.org/pod/LWP::UserAgent). Note: Mozilla::CA is needed to get around credential     ###
### requirements that come with accessing https servers. If not already installed, can be installed via ###
### the terminal with the command sudo cpan Mozilla::CA (make sure command line tools are installed).   ###
###########################################################################################################

# use proper packages
use LWP::UserAgent;
use Mozilla::CA;
use Cwd;

# change working directory
chdir('/hps/group/fuqua/mie4/EDGAR Parsing/Data/Master Files') or die "cannot change: $!\n";

# misc setup
my $ua = LWP::UserAgent->new; 
$ua->timeout(10);
$ua->env_proxy;

# iterate over years
for($year = 1993; $year < 2021; $year = $year + 1) {
    # iterate over quarter
    for($i = 1; $i < 5; $i = $i + 1) {
        # make file address
        $quarter = 'QTR' . $i;
        $filegrag = 'https://www.sec.gov/Archives/edgar/full-index/' . $year . '/' . $quarter . '/master.idx';

        # get file from EDGAR
        my $response = $ua->get($filegrag);

        # pipe the output to a file named appropriately
        $filename = $year . $quarter . 'master';
        open(MYOUTFILE, '>' . $filename);
        if($response->is_success) {
            print MYOUTFILE $response->decoded_content;
        } else {
            die $response->status_line;
        }
        close(MYOUTFILE);
    }
}
