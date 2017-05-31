#-----------------------------------------------------
package BFM::AUM::MVPublisher;
#-----------------------------------------------------
#
# Last edited by: $Author: rhoward $
#             on: $Date: 2015/07/28 14:13:21 $
#       Filename: $RCSfile: MVPublisher.pm,v $
#       Revision: $Revision: 1.15 $
#
#-----------------------------------------------------
=head1 NAME
BFM::AUM::MVPublisher
=head1 SYNOPSIS
    # Gather MV AUM-related information for a specific set of portfolios
    # on a specific month end
=head1 DESCRIPTION
    Module that regulates workflow for MV AUM publishing process.
    This module is the backbone of the MV AUM-related portion of the AUM Publisher GUI.
=head1 METHODS
=cut
use strict;
use warnings;
use vars qw($VERSION $STATUS $STATUS_LABELS $PERIOD_STATUS_LABELS $CLT_STATUS_LABELS $AUDIT_LOG $MAX_NUM_WORKING_SNAPS);
($VERSION) = ( qw$Revision: 1.15 $ )[1];
use Exporter;
use base qw(Exporter);
our @EXPORT = qw(mvAumCreateSnapshot 
    mvAumFormatFileName 
    mvAumFormatDirectoryName 
    mvAumUpdateStandardFile 
    mvAumUploadDoubleDipFile 
    mvAumUploadKpiFile 
    mvAumUploadSleeveFile 
    mvAumUploadSuppressionFile 
    mvAumRefreshSnapshot 
    mvAumAmvCubeFactsExtract 
    mvAumSendEmail 
    mvAumPublish 
    mvAumRunExceptionsCheck 
    mvAumOpenExcelReports 
    mvAumOpenSnapshot
    mvAumUploadFile 
    mvAumDownloadOverrideData 
    mvAumDownloadJacketsData 
    mvAumLogger 
    mvAumGetOptions 
    mvAumGetOptionsWithMode 
    mvAumGetMainData 
    mvAumFindBadPortfolioCodes 
    mvAumGetDifferenceReport 
    mvAumUploadTT1DifferenceFile 
    mvAumDownloadTT1DifferenceData 
    mvAumBuildDirectories 
    mvAumGenerateReports 
    mvAumOpenCrossHoldings 
    mvAumOpenWalkforward 
    mvAumUploadIsAllowed 
    mvAumDownloadKpiData 
    mvAumGetUser);
use Data::Dumper;
use BFMDateTime;
use DataObject;
use BFM::GetTable;
use BFM::AUM::CLTUpload;
use BFM::AUM::Snapshot;
use BFM::AUM::PortfolioList;
use BFM::Mail;
use BFM::LogNG;
use BFM::GetFile;
use OOBFMDate qw(:FMT);
use BFM::Util::Benchmark;
use HTML::Embperl;
use File::Slurp;
use BFM::TempFile;
use Date::Parse;
use File::Copy;
use BFM::AUM::Reports;
use BFM::SecureRandom;
use BFM::BCP;
use Text::CSV_XS;
use Log::Log4perl qw(:levels);
use BFM::AUM::CLTUpload::Constants;
use BFM::AUM::AUMFtpPoller;
use BFM::MIS::MisDispatcher;
use BFM::Query::assets;
use BFM::MIS::Constants;
use BFM::AUM::Reports::RegulatoryAUM;
use CGI;
use CGI::Carp qw (fatalsToBrowser);
use File::Basename;
use BFM::AUM::Publisher;
use BFM::AUM::MVAUM;
use BFM::AUM::MVExceptions;
use BFM::AUM::MVReports;
    
my $MVUPLOAD_DIR = BFM::GetFile::get_file('AUMReportDir') . '/manager_view';
my $MVDEBUG_FILE = "AUMPublisherMVDebug.log";
my $MVDEBUG_LOG  = $MVUPLOAD_DIR . '/' . $MVDEBUG_FILE;
# *************************************************************************************************
# begin mv aum
# *************************************************************************************************
=head2 C<mvAumCreateSnapshot($rhFormData, $CFG)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumCreateSnapshot
{
    my ($rhFormData, $CFG) = @_;
    my $port_date = $rhFormData->{'port_date'};
    my $rhData = &BFM::AUM::Publisher::getMainData($rhFormData, $CFG);
    $rhData->{'port_date'} = $port_date;
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    my $message = '';
    
    my $blockIsOK = eval 
    {
        my $oPublisher = BFM::AUM::Publisher->new($rhData, $rhFormData, $CFG);
        
        # *****************************************************************************************
        # *****************************************************************************************
        my $datatype = $rhFormData->{'data-datatype'} || $BFM::AUM::MVAUM::PURPOSE->{$oPublisher->{'snapshot_type'}}; 
        my $asof_date = $rhFormData->{'data-snapshot-date'} || $port_date;
        
        my $mode = 'create_snapshot';
        my $options = &mvAumGetOptionsWithMode($asof_date, $datatype, $user, $mode);
        # *****************************************************************************************
        # *****************************************************************************************
        
        # *****************************************************************************************
        # *****************************************************************************************
        my $oDispatcher = BFM::MIS::MisDispatcher->new();
        my $writeFileOK = $oDispatcher->writeFile('MVGeneric', $options);
        unless($writeFileOK) 
        {
            die($oDispatcher->{'ERROR'} . "\n");
        }
        # *****************************************************************************************
        # *****************************************************************************************
        
        1;
    };
    if($blockIsOK)
    {
        $message = 
            "$user has scheduled the creation of a snapshot on $today. An email will be sent upon completion. ";
        $rhData->{'MESSAGES'}->{'OK'} = $message;
        $rhData->{'MESSAGES'}->{'INFO'} = $message;
    }
    else
    {
        $message = $@;
        &mvAumLogger($message);
        $rhData->{'MESSAGES'}->{'ERROR'} = $message;
    }
    
    return $rhData;
} # end sub mvAumCreateSnapshot
=head2 C<mvAumFormatFileName($rawFileName, $fileNamePrefix)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumFormatFileName
{
    my ($rawFileName, $fileNamePrefix) = @_;
    
    # perl5.8 -e 'use BFM::AUM::Publisher;print(&BFM::AUM::Publisher::mvAumFormatFileName("abc.csv", "foobar_"), "\n");'
    
    my $fileName = $rawFileName;
    my $safe_filename_characters = "a-zA-Z0-9_.-";
    $fileName =~ s/ /_/g;
    $fileName =~ s/[^$safe_filename_characters]//g;
    my ($name, $path, $extension) = fileparse($fileName, '\..*');
    my $today = BFMDateTime->new();
    my $todayFmt = "$today";
    
    $todayFmt =~ s/[^$safe_filename_characters]//g;
    $fileName = "$fileNamePrefix" . "_" . "$todayFmt$extension";
    return $fileName;
} # end sub mvAumFormatFileName
=head2 C<mvAumFormatDirectoryName($portDate)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumFormatDirectoryName
{
    my ($portDate) = @_;
    
    my $portDateFmt = '';
    
    my $someday = BFMDate->new_from_string($portDate);
    $portDateFmt = sprintf("%04d%02d%02d", $someday->year(), $someday->month(), $someday->day());
    
    return $portDateFmt;
} # end sub mvAumFormatDirectoryName
=head2 C<mvAumUpdateStandardFile($directoryName, $fileNamePrefix, $latestFile)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumUpdateStandardFile
{
    my ($directoryName, $fileNamePrefix, $latestFile) = @_;
    
    my $message = '';
    my $standardFileName = "$directoryName/$fileNamePrefix.csv";
    my $from = "$directoryName/$latestFile";
    my $to = $standardFileName; 
    my $command = '';
    my $numberOfFilesDeleted = 0;
    my $blockIsOK = eval 
    {
        if(-e $to)
        {
            $numberOfFilesDeleted = unlink $to or die("Unable to unlink file $to: $!\n");
        }
        copy($from, $to) or die("Unable to copy file $from: $!\n");
        
        1;
    };
    if($blockIsOK)
    {
        $message = '';
        &mvAumLogger("Publisher::mvAumUpdateStandardFile. . .success: OK");
    }
    else
    {
        $message = "Publisher::mvAumUpdateStandardFile. . .error: $@";
        &mvAumLogger($message);
    }
    return $message;
} # end sub mvAumUpdateStandardFile
=head2 C<mvAumUploadDoubleDipFile($rhFormData, $CFG, $rhData)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumUploadDoubleDipFile
{
    my ($rhFormData, $CFG, $rhData) = @_;
    
    my $fieldName = $rhFormData->{'this_datafile_item'};
    my $inFH = $rhFormData->{$rhFormData->{'this_datafile_item'}};
    my $fileName = $BFM::AUM::MVAUM::MANUAL_DD_OVERRIDE_FILE;
    my $fileNamePrefix = $fileName;
    $fileNamePrefix =~ s/.csv//;
    my $fmtInFH = &mvAumFormatFileName($inFH, $fileNamePrefix);        
    
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    
    my $recordCount = 0;
    my @contents;
    
    my $contentsLength;
    my @fields;
    my $field;
    my $message = '';
    my $mergeMessage = '';
    my $renameMessage = '';
    my $fileIsOK = 0;
    my $openIsOK = 0;
    my $sql;
    my @listOfPortfolioCodes = ();
    my @listOfBadPortfolioCodes = ();
    my $NUMBER_OF_DOUBLE_DIP_COLUMNS = 9;
    my $DIRECTORY_PERMISSIONS = oct(777);
    
    my $blockIsOK = eval
    {
        my $port_date = $rhFormData->{port_date};
        my $port_date_directory_name = BFM::AUM::Publisher::mvAumFormatDirectoryName($port_date);
        if(!$fieldName)
        {
            $fileIsOK = 0;
            $message = "Publisher::mvAumUploadDoubleDipFile. . .file name is missing";
            &mvAumLogger($message);
            die $message;
        }
        # *****************************************************************************************
        # *****************************************************************************************
        my $directoriesStatus = &mvAumBuildDirectories($port_date);
        unless(exists $directoriesStatus->{'MESSAGES'}->{'OK'})
        {
            $message = 'There was an error setting up the report directory. ' .
                $directoriesStatus->{'MESSAGES'}->{'ERROR'};
            &mvAumLogger($message);
            die("$message\n");
        }
        my $uploadDirectory = $directoriesStatus->{'MESSAGES'}->{'UPLOAD_DIRECTORY'};
        # *****************************************************************************************
        # *****************************************************************************************
        
        my $cgiTempDir = $CGITempFile::TMPDIRECTORY;
        $CGITempFile::TMPDIRECTORY = $uploadDirectory;
        my $cgi = CGI->new();
        
        my $file = $cgi->upload($fieldName); 
        my $tmpfilename = $cgi->tmpFileName($inFH);        
        
        $recordCount = 0;
        @contents = read_file($file);
        
        $contentsLength = @contents;
        $message = '';
        $fileIsOK = 0;
        
        $fileIsOK = 1;
        foreach my $line(@contents)
        {
            if($recordCount == 0)
            {
                # validate header record
                @fields = split(',', $line);
                if(scalar(@fields) != $NUMBER_OF_DOUBLE_DIP_COLUMNS)
                {
                    $message = "Double dip file error: must have $NUMBER_OF_DOUBLE_DIP_COLUMNS columns for header. ";
                    $fileIsOK = 0;
                    last;
                }
                $field = $fields[0];
            }
            else
            {
                # validate data records
                @fields = split(',', $line);
                if(scalar(@fields) != $NUMBER_OF_DOUBLE_DIP_COLUMNS)
                {
                    $message = "Double dip file error: must have $NUMBER_OF_DOUBLE_DIP_COLUMNS columns for data. ";
                    $fileIsOK = 0;
                    last;
                }
            }
            $recordCount++;
        } # end foreach my $line(@contents)
        if($fileIsOK)
        {
            open(my $FH, ">", "$uploadDirectory/$fmtInFH") or die("Publisher::mvAumUploadDoubleDipFile. . .not openIsOK: $!\n");
            foreach my $line(@contents)
            {
                # make sure in Unix format
                $line =~ s/\r$//;
                print $FH "$line";
            } # end foreach my $line(@contents)
            close $FH;
            
            # do overwrite
            $renameMessage = &mvAumUpdateStandardFile($uploadDirectory, $fileNamePrefix, $fmtInFH);
            if($renameMessage)
            {
                die $renameMessage;
            }
        }
        else
        {
            #&mvAumLogger("Publisher::mvAumUploadDoubleDipFile. . .message: $message");
            $rhData->{'MESSAGES'}->{'ERROR'} = $message;
        } # end if($fileIsOK)
        #&mvAumLogger("Publisher::mvAumUploadDoubleDipFile. . .after loop");
        
        # is OK
        1;
    }; # end my $blockIsOK = eval
    
    if($blockIsOK)
    {
        # exception not thrown: something went right
        if($fileIsOK)
        {
            $rhData->{'MESSAGES'}->{'OK'} = "Double dip file uploaded. ";
        }
        else
        {
            &mvAumLogger("Double dip file error: $message");
            $rhData->{'MESSAGES'}->{'ERROR'} = $message;
        }
    }
    else
    {
        $message = $@;
        # exception thrown: something went wrong
        &mvAumLogger("Publisher::mvAumUploadDoubleDipFile. . .: $message");
        $rhData->{'MESSAGES'}->{'ERROR'} = "Double dip file exception: $message";
    } # end if($blockIsOK)
    return $rhData;
    
} # end sub mvAumUploadDoubleDipFile
=head2 C<mvAumUploadKpiFile($rhFormData, $CFG, $rhData)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumUploadKpiFile
{
    my ($rhFormData, $CFG, $rhData) = @_;
    
    my $fieldName = $rhFormData->{'this_datafile_item'};
    my $inFH = $rhFormData->{$rhFormData->{'this_datafile_item'}};
    my $fileName = $BFM::AUM::MVAUM::KPI_OVERRIDE_FILE;
    my $fileNamePrefix = $fileName;
    $fileNamePrefix =~ s/.csv//;
    my $fmtInFH = &mvAumFormatFileName($inFH, $fileNamePrefix);
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    
    my $recordCount = 0;
    my @contents;
    
    my $contentsLength;
    my @fields;
    my $field;
    my $message = '';
    my $mergeMessage = '';
    my $renameMessage = '';
    my $fileIsOK = 0;
    my $openIsOK = 0;
    my $sql;
    my @listOfPortfolioCodes = ();
    my @listOfBadPortfolioCodes = ();
    my $NUMBER_OF_FILE_COLUMNS = 15;
    my $DIRECTORY_PERMISSIONS = oct(777);
    
    my $blockIsOK = eval
    {
        my $port_date = $rhFormData->{port_date};
        my $port_date_directory_name = BFM::AUM::Publisher::mvAumFormatDirectoryName($port_date);
        
        if(!$fieldName)
        {
            $fileIsOK = 0;
            $message = "Publisher::mvAumUploadKpiFile. . .file name is missing";
            &mvAumLogger($message);
            die $message;
        }
        # *****************************************************************************************
        # *****************************************************************************************
        my $directoriesStatus = &mvAumBuildDirectories($port_date);
        unless(exists $directoriesStatus->{'MESSAGES'}->{'OK'})
        {
            $message = 'There was an error setting up the report directory. ' .
                $directoriesStatus->{'MESSAGES'}->{'ERROR'};
            &mvAumLogger($message);
            die("$message\n");
        }
        my $uploadDirectory = $directoriesStatus->{'MESSAGES'}->{'UPLOAD_DIRECTORY'};
        # *****************************************************************************************
        # *****************************************************************************************
        
        my $cgiTempDir = $CGITempFile::TMPDIRECTORY;
        $CGITempFile::TMPDIRECTORY = $uploadDirectory;
        my $cgi = CGI->new();
        
        my $file = $cgi->upload($fieldName); 
        my $tmpfilename = $cgi->tmpFileName($inFH);        
        
        $recordCount = 0;
        @contents = read_file($file);
        
        $contentsLength = @contents;
        $message = '';
        $fileIsOK = 0;
        
        $fileIsOK = 1;
        foreach my $line(@contents)
        {
            if($recordCount == 0)
            {
                # validate header record
                @fields = split(',', $line);
                if(scalar(@fields) != $NUMBER_OF_FILE_COLUMNS)
                {
                    $message = "KPI file error: must have $NUMBER_OF_FILE_COLUMNS columns for header. ";
                    $fileIsOK = 0;
                    last;
                }
                $field = $fields[0];
                
                if($field !~ m/portfolio_code/i)
                {
                    $message = "KPI file error: first column must have title 'portfolio_code'. ";
                    $fileIsOK = 0;
                    last;
                }
            }
            else
            {
                # validate data records
                @fields = split(',', $line);
                if(scalar(@fields) != $NUMBER_OF_FILE_COLUMNS)
                {
                    $message = "KPI file error: must have $NUMBER_OF_FILE_COLUMNS columns for data. ";
                    $fileIsOK = 0;
                    last;
                }
                $field = $fields[0];
                if($field !~ m/-?\d+(\.\d+)?/)
                {
                    $message = "KPI file error: invalid value found - $field. ";
                    $fileIsOK = 0;
                    last;
                }
            }
            $recordCount++;
        } # end foreach my $line(@contents)
        
        if($fileIsOK)
        {
            open(my $FH, ">", "$uploadDirectory/$fmtInFH") or die("Publisher::mvAumUploadKpiFile. . .not openIsOK: $!\n");
            foreach my $line(@contents)
            {
                # make sure in Unix format
                $line =~ s/\r$//;
                print $FH "$line";
            } # end foreach my $line(@contents)
            close $FH;
            
            # do overwrite
            $renameMessage = &mvAumUpdateStandardFile($uploadDirectory, $fileNamePrefix, $fmtInFH);
            if($renameMessage)
            {
                die $renameMessage;
            }
            
            # *************************************************************************************
            # *************************************************************************************
            my $oPublisher = BFM::AUM::Publisher->new($rhData, $rhFormData, $CFG);
            my $options = &mvAumGetOptions($rhFormData, $CFG);
            my $oMvAum = BFM::AUM::MVAUM->new($options);
            if($oMvAum->is_published())
            {
                &mvAumLogger("Publisher::mvAumUploadKpiFile. . .business error: snapshot is published");
                $message = 'Cannot upload KPI override on published snapshot. ';
                die "$message\n";
            }
            else
            {
                # *****************************************************************************************
                # *****************************************************************************************
                my $datatype = $rhFormData->{'data-datatype'} || $BFM::AUM::MVAUM::PURPOSE->{$oPublisher->{'snapshot_type'}}; 
                my $asof_date = $rhFormData->{'data-snapshot-date'} || $port_date;
                
                my $mode = 'load_kpi';
                my $optionsWithMode = &mvAumGetOptionsWithMode($asof_date, $datatype, $user, $mode);
                # *****************************************************************************************
                # *****************************************************************************************
                
                # *****************************************************************************************
                # *****************************************************************************************
                my $oDispatcher = BFM::MIS::MisDispatcher->new();
                my $writeFileOK = $oDispatcher->writeFile('MVGeneric', $optionsWithMode);
                unless($writeFileOK) 
                {
                    die($oDispatcher->{'ERROR'} . "\n");
                }
                
                $message = 
                    "$user has scheduled the processing of the uploaded KPI file on $today. An email will be sent upon completion. ";
                
                # *****************************************************************************************
                # *****************************************************************************************
            }
            # *************************************************************************************
            # *************************************************************************************
        }
        else
        {
            &mvAumLogger("Publisher::mvAumUploadKpiFile. . .message: $message");
            $rhData->{'MESSAGES'}->{'ERROR'} = $message;
        } # end if($fileIsOK)
        
        1;
    }; # end my $blockIsOK = eval
    
    if($blockIsOK)
    {
        # exception not thrown: something went right
        if($fileIsOK)
        {
            $rhData->{'MESSAGES'}->{'OK'} = $message;
        }
        else
        {
            &mvAumLogger("KPI file error: $message");
            $rhData->{'MESSAGES'}->{'ERROR'} = $message;
        }
    }
    else
    {
        $message = $@;
        # exception thrown: something went wrong
        &mvAumLogger("Publisher::mvAumUploadKpiFile. . .: $message");
        $rhData->{'MESSAGES'}->{'ERROR'} = "KPI file exception: $message";
    } # end if($blockIsOK)
    return $rhData;
} # end sub mvAumUploadKpiFile
=head2 C<mvAumUploadSleeveFile($rhFormData, $CFG, $rhData)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumUploadSleeveFile
{
    my ($rhFormData, $CFG, $rhData) = @_;
    
    my $fieldName = $rhFormData->{'this_datafile_item'};
    my $inFH = $rhFormData->{$rhFormData->{'this_datafile_item'}};
    my $fileName = $BFM::AUM::MVAUM::SLV_OVERRIDE_FILE;
    my $fileNamePrefix = $fileName;
    $fileNamePrefix =~ s/.csv//;
    my $fmtInFH = &mvAumFormatFileName($inFH, $fileNamePrefix);        
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    
    my $recordCount = 0;
    my @contents;
    
    my $contentsLength;
    my @fields;
    my $field;
    my $message = '';
    my $mergeMessage = '';
    my $renameMessage = '';
    my $fileIsOK = 0;
    my $openIsOK = 0;
    my $sql;
    my @listOfPortfolioCodes = ();
    my @listOfBadPortfolioCodes = ();
    my $NUMBER_OF_FILE_COLUMNS = 7;
    my $DIRECTORY_PERMISSIONS = oct(777);
    
    my $blockIsOK = eval
    {
        my $port_date = $rhFormData->{port_date};
        my $port_date_directory_name = BFM::AUM::Publisher::mvAumFormatDirectoryName($port_date);
        if(!$fieldName)
        {
            $fileIsOK = 0;
            $message = "Publisher::mvAumUploadSleeveFile. . .file name is missing";
            &mvAumLogger($message);
            die $message;
        }
        # *****************************************************************************************
        # *****************************************************************************************
        my $directoriesStatus = &mvAumBuildDirectories($port_date);
        unless(exists $directoriesStatus->{'MESSAGES'}->{'OK'})
        {
            $message = 'There was an error setting up the report directory. ' .
                $directoriesStatus->{'MESSAGES'}->{'ERROR'};
            &mvAumLogger($message);
            die("$message\n");
        }
        my $uploadDirectory = $directoriesStatus->{'MESSAGES'}->{'UPLOAD_DIRECTORY'};
        # *****************************************************************************************
        # *****************************************************************************************
        my $cgiTempDir = $CGITempFile::TMPDIRECTORY;
        $CGITempFile::TMPDIRECTORY = $uploadDirectory;
        my $cgi = CGI->new();
        
        my $file = $cgi->upload($fieldName); 
        my $tmpfilename = $cgi->tmpFileName($inFH);        
        
        $recordCount = 0;
        @contents = read_file($file);
        
        $contentsLength = @contents;
        $message = '';
        $fileIsOK = 0;
        
        $fileIsOK = 1;
        foreach my $line(@contents)
        {
            if($recordCount == 0)
            {
                # validate header record
                @fields = split(',', $line);
                if(scalar(@fields) != $NUMBER_OF_FILE_COLUMNS)
                {
                    $message = "Sleeve file error: must have $NUMBER_OF_FILE_COLUMNS columns for header. ";
                    $fileIsOK = 0;
                    last;
                }
                $field = $fields[0];
                
                if($field !~ m/value/i)
                {
                    $message = "Sleeve file error: first column must have title 'VALUE'. ";
                    $fileIsOK = 0;
                    last;
                }
            }
            else
            {
                # validate data records
                @fields = split(',', $line);
                if(scalar(@fields) != $NUMBER_OF_FILE_COLUMNS)
                {
                    $message = "Sleeve file error: must have $NUMBER_OF_FILE_COLUMNS columns for data. ";
                    $fileIsOK = 0;
                    last;
                }
                $field = $fields[0];
                if($field !~ m/-?\d+(\.\d+)?/)
                {
                    $message = "Sleeve file error: invalid value found - $field. ";
                    $fileIsOK = 0;
                    last;
                }
            }
            $recordCount++;
        } # end foreach my $line(@contents)
        
        if($fileIsOK)
        {
            open(my $FH, ">", "$uploadDirectory/$fmtInFH") or die("Publisher::mvAumUploadSleeveFile. . .not openIsOK: $!\n");
            foreach my $line(@contents)
            {
                # make sure in Unix format
                $line =~ s/\r$//;
                print $FH "$line";
            } # end foreach my $line(@contents)
            close $FH;
            
            # do overwrite
            $renameMessage = &mvAumUpdateStandardFile($uploadDirectory, $fileNamePrefix, $fmtInFH);
            if($renameMessage)
            {
                die $renameMessage;
            }
        }
        else
        {
            $rhData->{'MESSAGES'}->{'ERROR'} = $message;
        } # end if($fileIsOK)
        
        # is OK
        1;
    }; # end my $blockIsOK = eval
    
    if($blockIsOK)
    {
        # exception not thrown: something went right
        if($fileIsOK)
        {
            #&mvAumLogger("Publisher::mvAumUploadSleeveFile. . .: OK");
            $rhData->{'MESSAGES'}->{'OK'} = "Sleeve file uploaded. ";
        }
        else
        {
            #&mvAumLogger("Sleeve file error: $message");
            $rhData->{'MESSAGES'}->{'ERROR'} = $message;
        }
    }
    else
    {
        $message = $@;
        # exception thrown: something went wrong
        &mvAumLogger("Publisher::mvAumUploadSleeveFile. . .: $message");
        $rhData->{'MESSAGES'}->{'ERROR'} = "Sleeve file exception: $message. ";
    } # end if($blockIsOK)
    return $rhData;
    
} # end sub mvAumUploadSleeveFile
=head2 C<mvAumUploadSuppressionFile($rhFormData, $CFG, $rhData)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumUploadSuppressionFile
{
    my ($rhFormData, $CFG, $rhData) = @_;
    
    my $fieldName = $rhFormData->{'this_datafile_item'};
    my $inFH = $rhFormData->{$rhFormData->{'this_datafile_item'}};
    my $fileName = $BFM::AUM::MVAUM::SUPP_JACKETS_FILE;
    my $fileNamePrefix = $fileName;
    $fileNamePrefix =~ s/.csv//;
    my $fmtInFH = &mvAumFormatFileName($inFH, $fileNamePrefix);        
    
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    
    my $recordCount = 0;
    my @contents;
    
    my $contentsLength;
    my @fields;
    my $field;
    my $message = '';
    my $mergeMessage = '';
    my $renameMessage = '';
    my $fileIsOK = 0;
    my $openIsOK = 0;
    my $sql;
    my @listOfPortfolioCodes = ();
    my @listOfBadPortfolioCodes = ();
    my $NUMBER_OF_FILE_COLUMNS = 1;
    my $DIRECTORY_PERMISSIONS = oct(777);
    
    my $blockIsOK = eval
    {
        my $port_date = $rhFormData->{port_date};
        my $port_date_directory_name = BFM::AUM::Publisher::mvAumFormatDirectoryName($port_date);
    
        if(!$fieldName)
        {
            $fileIsOK = 0;
            $message = "Publisher::mvAumUploadSuppressionFile. . .file name is missing";
            &mvAumLogger($message);
            die $message;
        }
    
        # *****************************************************************************************
        # *****************************************************************************************
        my $directoriesStatus = &mvAumBuildDirectories($port_date);
        unless(exists $directoriesStatus->{'MESSAGES'}->{'OK'})
        {
            $message = 'There was an error setting up the report directory. ' .
                $directoriesStatus->{'MESSAGES'}->{'ERROR'};
            &mvAumLogger($message);
            die("$message\n");
        }
        my $uploadDirectory = $directoriesStatus->{'MESSAGES'}->{'UPLOAD_DIRECTORY'};
        # *****************************************************************************************
        # *****************************************************************************************
        
        my $cgiTempDir = $CGITempFile::TMPDIRECTORY;
        $CGITempFile::TMPDIRECTORY = $uploadDirectory;
        my $cgi = CGI->new();
        
        my $file = $cgi->upload($fieldName); 
        my $tmpfilename = $cgi->tmpFileName($inFH);        
        
        $recordCount = 0;
        @contents = read_file($file);
        
        $contentsLength = @contents;
        $message = '';
        $fileIsOK = 0;
        
        $fileIsOK = 1;
        foreach my $line(@contents)
        {
            if($recordCount == 0)
            {
                # validate header record
                @fields = split(',', $line);
                if(scalar(@fields) != $NUMBER_OF_FILE_COLUMNS)
                {
                    $message = "Suppression file error: file must have $NUMBER_OF_FILE_COLUMNS columns for header. ";
                    $fileIsOK = 0;
                    last;
                }
                $field = $fields[0];
                
                if($field !~ m/portfolio_code/i)
                {
                    $message = "Suppression file error: must have one column called 'portfolio_code'. ";
                    $fileIsOK = 0;
                    last;
                }
            }
            else
            {
                # validate data records
                @fields = split(',', $line);
                if(scalar(@fields) != $NUMBER_OF_FILE_COLUMNS)
                {
                    $message = "Suppression file error: must have one column for data. ";
                    $fileIsOK = 0;
                    last;
                }
                $field = $fields[0];
                if($field !~ m/-?\d+/)
                {
                    $message = "Suppression file error: invalid portfolio code - $field. ";
                    $fileIsOK = 0;
                    last;
                }
            }
            $recordCount++;
        } # end foreach my $line(@contents)
        # assemble list of portfolio codes for sql statement
        if($fileIsOK)
        {
            $recordCount = 0;
            foreach my $line(@contents)
            {
                if($recordCount == 0)
                {
                    $recordCount++;
                    next;
                }
                
                #print LOCAL "$line\n";
                ##&mvAumLogger("Publisher::mvAumUploadSuppressionFile. . .after $recordCount: $line");
                
                @fields = split(',', $line);
                $field = $fields[0];
                ##&mvAumLogger("Publisher::mvAumUploadSuppressionFile. . .field: $field");
                
                #$listOfPortfolioCodes .= "$field,";
                push @listOfPortfolioCodes, $field;
                
                $recordCount++;
            } # end foreach my $line(@contents)
            #$listOfPortfolioCodes = chop($listOfPortfolioCodes);
            
            # build and run sql statement to validate list of portfolio codes
            @listOfBadPortfolioCodes = &BFM::AUM::Publisher::mvAumFindBadPortfolioCodes(@listOfPortfolioCodes);
            ##&mvAumLogger("Publisher::mvAumUploadSuppressionFile. . .listOfBadPortfolioCodes: @listOfBadPortfolioCodes");
            
            $fileIsOK = 1;
            my $listOfBadPortfolioCodesLength = @listOfBadPortfolioCodes;
            ##&mvAumLogger("Publisher::mvAumUploadSuppressionFile. . .listOfBadPortfolioCodesLength: $listOfBadPortfolioCodesLength");
            if($listOfBadPortfolioCodesLength)
            {
                $fileIsOK = 0;
                $message = "Suppresion file error: portfolio codes not found - @listOfBadPortfolioCodes. ";
                # die $message;
            }
        } # end if($fileIsOK)
        
        if($fileIsOK)
        {
            open(my $FH, ">", "$uploadDirectory/$fmtInFH") or die("Publisher::mvAumUploadKpiFile. . .not openIsOK: $!\n");
            foreach my $line(@contents)
            {
                # make sure in Unix format
                $line =~ s/\r$//;
                print $FH "$line";
            } # end foreach my $line(@contents)
            close $FH;
            
            # do overwrite
            $renameMessage = &mvAumUpdateStandardFile($uploadDirectory, $fileNamePrefix, $fmtInFH);
            if($renameMessage)
            {
                die $renameMessage;
            }
        }
        else
        {
            #&mvAumLogger("Publisher::mvAumUploadSuppressionFile. . .message: $message");
            $rhData->{'MESSAGES'}->{'ERROR'} = $message;
        } # end if($fileIsOK)
        ##&mvAumLogger("Publisher::mvAumUploadSuppressionFile. . .after loop");
        
        # is OK
        1;
    }; # end my $blockIsOK = eval
    
    if($blockIsOK)
    {
        # exception not thrown: something went right
        if($fileIsOK)
        {
            &mvAumGetDifferenceReport($rhFormData, $CFG);
            
            $rhData->{'MESSAGES'}->{'OK'} = "Suppression file uploaded and report email sent. ";
            $rhData->{'MESSAGES'}->{'INFO'} = "Suppression file uploaded and report email sent. ";
        }
        else
        {
            &mvAumLogger("Suppression file error: $message");
            $rhData->{'MESSAGES'}->{'ERROR'} = $message;
        }
    }
    else
    {
        $message = $@;
        # exception thrown: something went wrong
        &mvAumLogger("Publisher::mvAumUploadSuppressionFile. . .: $message");
        $rhData->{'MESSAGES'}->{'ERROR'} = "Suppression file exception: $message. ";
    } # end if($blockIsOK)
    return $rhData;
} # end sub mvAumUploadSuppressionFile
=head2 C<mvAumRefreshSnapshot($rhFormData, $CFG)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumRefreshSnapshot
{
    my ($rhFormData, $CFG) = @_;
    my $port_date = $rhFormData->{'port_date'};
    my $rhData = &BFM::AUM::Publisher::getMainData($rhFormData, $CFG);
    $rhData->{'port_date'} = $port_date;
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    my $message = '';
    
    my $blockIsOK = eval 
    {
        my $oPublisher = BFM::AUM::Publisher->new($rhData, $rhFormData, $CFG);
        
        # *****************************************************************************************
        # *****************************************************************************************
        my $datatype = $rhFormData->{'data-datatype'} || $BFM::AUM::MVAUM::PURPOSE->{$oPublisher->{'snapshot_type'}}; 
        my $asof_date = $rhFormData->{'data-snapshot-date'} || $port_date;
        
        my $mode = 'create_snapshot';
        my $options = &mvAumGetOptionsWithMode($asof_date, $datatype, $user, $mode);
        # *****************************************************************************************
        # *****************************************************************************************
        
        # *****************************************************************************************
        # *****************************************************************************************
        my $oDispatcher = BFM::MIS::MisDispatcher->new();
        my $writeFileOK = $oDispatcher->writeFile('MVGeneric', $options);
        unless($writeFileOK) 
        {
            die($oDispatcher->{'ERROR'} . "\n");
        }
        # *****************************************************************************************
        # *****************************************************************************************
        
        1;
    };
    if($blockIsOK)
    {
        $message = 
            "$user has scheduled the creation of a snapshot on $today. An email will be sent upon completion. ";
        $rhData->{'MESSAGES'}->{'OK'} = $message;
        $rhData->{'MESSAGES'}->{'INFO'} = $message;
    }
    else
    {
        $message = $@;
        &mvAumLogger($message);
        $rhData->{'MESSAGES'}->{'ERROR'} = $message;
    }
    
    return $rhData;
} # end sub mvAumRefreshSnapshot
=head2 C<mvAumAmvCubeFactsExtract($rhFormData, $CFG)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumAmvCubeFactsExtract
{
    my ($rhFormData, $CFG) = @_;
    my $port_date = $rhFormData->{'port_date'};
    my $rhData = &BFM::AUM::Publisher::getMainData($rhFormData, $CFG);
    $rhData->{'port_date'} = $port_date;
    
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    my $todayFmt = $today->text_fmt($FMT_Slash);
    my $information = '';
    
    # validate purpose/datatype
    my $options = &mvAumGetOptions($rhFormData, $CFG);
    
    my $dump = Dumper $options;
    &mvAumLogger("options: $dump");
    
    
    # in aum_cross_holdings, purpose  => WEEKLY,MONTHLY
    # in aum_snapshot, datatype => AUM_MV_MONTHLY,AUM_MV_WEEKLY
    my $purpose  = $BFM::AUM::MVAUM::HOLDING_PURPOSE->{$options->{'purpose'}};
    my $datatype = $options->{'purpose'};
    my $status   = $options->{'status'};
    if($options->{'purpose'} ne $BFM::AUM::MVAUM::PURPOSE->{'MONTHLY'})
    {
        $information = "Only monthly cubes are available.";
        &mvAumLogger($information);
        $rhData->{'MESSAGES'}->{'ERROR'} = $information;
        return $rhData;
    }
    
    # set up dispatcher call
    my $mailList = BFM::GetFile::get_file("NstarEmailList");
    &mvAumLogger("mvAumAmvCubeFactsExtract: before new dispatcher. . .");
    my $oDispatcher = BFM::MIS::MisDispatcher->new();
    &mvAumLogger("mvAumAmvCubeFactsExtract: after new dispatcher. . .");
    my @segments = split(/\//, $rhFormData->{'port_date'});
    my $month = $segments[0];
    my $year  = $segments[2];
    my %opts = (
        'mis_status_mailto' => $mailList,
        'NstarExtractDir'   => BFM::GetFile::get_file("NstarExtractDir"),
        'aum_date'          => $rhFormData->{'port_date'},
        
        # *****************************************************************************************
        # hard coded tactical solution only
        # will devise algorithm for future release
        # *****************************************************************************************
        # 'AMV_START_DATE'    => '01/31/2015',
        'AMV_START_DATE'    => $CFG->{'mv_aum'}->{'AMV_START_DATE'},
        
        # *****************************************************************************************
        # hard coded tactical solution only
        # will devise algorithm for future release
        # *****************************************************************************************
        
        'AMV_END_DATE'      => $rhFormData->{'port_date'},
        'AMV_MONTH'         => $month,
        'AMV_YEAR'          => $year,
        'AMV_STATUS'        => $status,
        'AMV_DATATYPE'      => $datatype,
        'AMV_PURPOSE'       => $purpose,
    );
    
    # writeFile returns error status
    # &mvAumLogger("mvAumAmvCubeFactsExtract: before write file. . .");
    my $writeFileOK = $oDispatcher->writeFile('HierQCExtHierBuildQCAMV', \%opts);
    # &mvAumLogger("mvAumAmvCubeFactsExtract: after write file. . .");
    
    if($writeFileOK) 
    {
        $information = "$user has created a cube process on $todayFmt";
        # &mvAumLogger($information);
        $rhData->{'MESSAGES'}->{'OK'}   = $information;
        $rhData->{'MESSAGES'}->{'INFO'} = $information;
        
        my $from = BFM::GetFile::get_file('MV_AUM_PUBLISHER_FROM');
        my $to = BFM::GetFile::get_file('NstarEmailList');
        my $subject = 'Create EssBase Cube';
        my $message = "$user has created an EssBase cube for $port_date run on $todayFmt";
        &mvAumSendEmail($rhFormData, $CFG, $from, $to, $subject, $message);
        
    }
    else
    {
        $dump = Dumper %opts;
        &mvAumLogger("opts: $dump");
        
        $information = $oDispatcher->{'ERROR'};
        &mvAumLogger($information);
        $rhData->{'MESSAGES'}->{'ERROR'} = $information;
    }
    return $rhData;
} # end sub mvAumAmvCubeFactsExtract
=head2 C<mvAumSendEmail($rhFormData, $CFG, $from, $to, $subject, $message)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumSendEmail
{
    my ($rhFormData, $CFG, $from, $to, $subject, $message) = @_;
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    my $todayFmt = $today->text_fmt($FMT_Slash);
    my $information = "$user has sent an \nemail\n on $todayFmt";
    
    unless(defined $from)
    {
        $from = 'AUM Publisher';
    }
    unless(defined $to)
    {
        $to = BFM::GetFile::get_file('MV_AUM_EMAIL_PUBLISH');
    }
    unless(defined $subject)
    {
        $subject = 'AUM Publisher Email';
    }
    unless(defined $message)
    {
        $message = $information;
    }
    
    my $port_date = $rhFormData->{'port_date'};
    my $rhData = &BFM::AUM::Publisher::getMainData($rhFormData, $CFG);
    $rhData->{'port_date'} = $port_date;
    my $mailHash = {
        'from'      => $from,
        'to'        => $to,
        'testENVTo' => $user,
        'subject'   => $subject,
    };
    my $error = BFM::Mail::SendMail($mailHash, $message);
    if($error) 
    {
        $information = "Email Not Sent: $error";
        &mvAumLogger($information);
        $rhData->{'MESSAGES'}->{'ERROR'} = $information;
    }
    else 
    {
        $information = "$user has sent an email on $todayFmt";
        $rhData->{'MESSAGES'}->{'OK'} = $information;
    }
    return $rhData;
} # end sub mvAumSendEmail
=head2 C<mvAumPublish($rhFormData, $CFG)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumPublish
{
    my ($rhFormData, $CFG) = @_;
    my $port_date = $rhFormData->{'port_date'};
    my $rhData = &BFM::AUM::Publisher::getMainData($rhFormData, $CFG);
    $rhData->{'port_date'} = $port_date;
    my $oPublisher = BFM::AUM::Publisher->new($rhData, $rhFormData, $CFG);
    
    my $information = '';
    
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    my $todayFmt = $today->text_fmt($FMT_Slash);
    
    # *********************************************************************************************
    my $options = &mvAumGetOptions($rhFormData, $CFG);
    my $blockIsOK = eval 
    {
        my $oMvAum = BFM::AUM::MVAUM->new($options);
        my $isPublished = $oMvAum->is_published();
        
        unless($isPublished)
        {
            $oMvAum->publish_snapshot();
            
            my $isPublishedRecheck = $oMvAum->is_published();
            if($isPublishedRecheck)
            {
                my $from = BFM::GetFile::get_file('MV_AUM_PUBLISHER_FROM');
                my $to = BFM::GetFile::get_file('MV_AUM_EMAIL_PUBLISH');
                my $subject = 'Publish Managed View Snapshot';
                my $message = "$user has published a Managed View snapshot of $port_date";
                &mvAumSendEmail($rhFormData, $CFG, $from, $to, $subject, $message);
            }
            else
            {
                die("The snapshot was not published.\n");
            }
            
            if($isPublishedRecheck)
            {
                &mvAumAmvCubeFactsExtract($rhFormData, $CFG);
            }
            else
            {
                die("The snapshot was not published. No cube was created.\n");
            }
        }
        else
        {
            &mvAumLogger("Publisher::mvAumPublish. . .snapshot is already published");
            die "Snapshot is already published.\n";
        }
        
        1;
    };
    if($blockIsOK)
    {
        $information = "$user has published a MV AUM snapshot of $port_date run on $todayFmt";
        $rhData->{'MESSAGES'}->{'OK'} = "$information";
        $rhData->{'MESSAGES'}->{'INFO'} = "$information";
    }
    else
    {
        $information = "Publisher::mvAumPublish exception: $@";
        &mvAumLogger($information);
        $information = "$user has encountered an exception when publishing a MV AUM snapshot on $todayFmt";
        $rhData->{'MESSAGES'}->{'ERROR'} = "$information";
    }
    # *********************************************************************************************
    
    return $rhData;
} # end sub mvAumPublish
=head2 C<mvAumRunExceptionsCheck($rhFormData, $CFG)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumRunExceptionsCheck
{
    my ($rhFormData, $CFG) = @_;
    my $port_date = $rhFormData->{'port_date'};
    my $rhData = &BFM::AUM::Publisher::getMainData($rhFormData, $CFG);
    $rhData->{'port_date'} = $port_date;
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    my $message = '';
    
    my $blockIsOK = eval 
    {
        my $oPublisher = BFM::AUM::Publisher->new($rhData, $rhFormData, $CFG);
    
        # *****************************************************************************************
        # *****************************************************************************************
        my $datatype = $rhFormData->{'data-datatype'} || $BFM::AUM::MVAUM::PURPOSE->{$oPublisher->{'snapshot_type'}}; 
        my $asof_date = $rhFormData->{'data-snapshot-date'} || $port_date;
        
        my $mode = 'run_exceptions';
        my $options = &mvAumGetOptionsWithMode($asof_date, $datatype, $user, $mode);
        # *****************************************************************************************
        # *****************************************************************************************
        # *****************************************************************************************
        # *****************************************************************************************
        my $oDispatcher = BFM::MIS::MisDispatcher->new();
        my $writeFileOK = $oDispatcher->writeFile('MVGeneric', $options);
        unless($writeFileOK) 
        {
            die($oDispatcher->{'ERROR'} . "\n");
        }
        # *****************************************************************************************
        # *****************************************************************************************
        
        1;
    };
    if($blockIsOK)
    {
        $message = 
            "$user has scheduled an exceptions check on $today. An email will be sent upon completion. ";
        $rhData->{'MESSAGES'}->{'OK'} = $message;
        $rhData->{'MESSAGES'}->{'INFO'} = $message;
    }
    else
    {
        $message = $@;
        &mvAumLogger($message);
        $rhData->{'MESSAGES'}->{'ERROR'} = $message;
    }
    
    return $rhData;
} # end sub mvAumRunExceptionsCheck
=head2 C<mvAumOpenExcelReports($rhFormData, $CFG, $rhData, $fileName)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumOpenExcelReports
{
    my ($rhFormData, $CFG, $rhData, $fileName) = @_;
    $rhData = $rhFormData;
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    my $message = '';
    
    my $blockIsOK = eval 
    {
        my $oPublisher = BFM::AUM::Publisher->new($rhData, $rhFormData, $CFG);
        my $datatype = $rhFormData->{'data-datatype'} || $BFM::AUM::MVAUM::PURPOSE->{$oPublisher->{'snapshot_type'}}; 
        
        my $fileNamePrefix = $fileName;
        $fileNamePrefix =~ s/.csv//;
        
        my $directoryName = &mvAumFormatDirectoryName($rhFormData->{'data-snapshot-date'});
        my $fileToDownload = $MVUPLOAD_DIR . '/' . $directoryName . '/reports/' . $fileName;
        my $webTempDir = BFM::GetFile::get_file('WebTempDir');
        my $from = $fileToDownload;
        my $snapshotFileName = $fileNamePrefix . reverse(time) . '.csv'; 
        my $to = "$webTempDir/$snapshotFileName";
        if(-e $from)
        {
            $message = 'Cannot copy report file';
            copy($from, $to) or die "$message: $from\n";
            if(-e $fileToDownload)
            {
                $rhData->{'open_report'} = $snapshotFileName;
            }
            else
            {
                &mvAumLogger("Publisher::mvAumOpenExcelReports. . .report does not exist: $fileToDownload||$snapshotFileName");
                die($user . ' the report file does not exist on ' . $today . "\n");
            }
        }
        else
        {
            &mvAumGenerateReports($rhFormData, $CFG);
            if(-e $from)
            {
                $message = 'Cannot copy report file after generating reports';
                copy($from, $to) or die "$message: $from\n";
                if(-e $fileToDownload)
                {
                    $rhData->{'open_report'} = $snapshotFileName;
                }
                else
                {
                    &mvAumLogger("Publisher::mvAumOpenExcelReports. . .report does not exist after generate: $fileToDownload||$snapshotFileName");
                    die($user . ' the report file does not exist on ' . $today);
                }
            }
            else
            {
                &mvAumLogger("Publisher::mvAumOpenExcelReports. . .does not exist after generate: $fileToDownload||$snapshotFileName");
                die($user . ' the report file does not exist on ' . $today);
            }
        }
        
        1;
    };  # end my $blockIsOK = eval
    if($blockIsOK)
    {
        $message = $user . " generated and downloaded a report on " . $today;
        $rhData->{'MESSAGES'}->{'OK'} = $message;
        $rhData->{'MESSAGES'}->{'INFO'} = $message;
    }
    else
    {
        $message = $@;
        &mvAumLogger($message);
        $rhData->{'MESSAGES'}->{'ERROR'} = $message;
    }
    # *********************************************************************************************
    
    return $rhData;
} # end sub mvAumOpenExcelReports
=head2 C<mvAumOpenSnapshot($rhFormData, $CFG, $rhData)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumOpenSnapshot
{
    my ($rhFormData, $CFG, $rhData) = @_;
    $rhData = $rhFormData;
    my $fileName = $BFM::AUM::MVReports::SNAPSHOT_FILE;
    
    my $rhMessage = &mvAumOpenExcelReports($rhFormData, $CFG, $rhData, $fileName);
    
    if($rhMessage->{'MESSAGES'}->{'OK'})
    {
        $rhData->{'MESSAGES'}->{'OK'} = $rhMessage->{'MESSAGES'}->{'OK'};
        $rhData->{'MESSAGES'}->{'INFO'} = $rhMessage->{'MESSAGES'}->{'OK'};
    }
    else
    {
        $rhData->{'MESSAGES'}->{'ERROR'} = $rhMessage->{'MESSAGES'}->{'ERROR'};
    }
    return $rhData;
} # end sub mvAumOpenSnapshot
=head2 C<mvAumUploadFile($rhFormData, $CFG, $rhData)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumUploadFile
{
    my ($rhFormData, $CFG, $rhData) = @_;
    # *********************************************************************************************
    # Upload control to standard file name mapping
    # *********************************************************************************************
    # Double Dip                                => mv_dd_overrides.csv       => datafile_double_dip
    # Suppressed Jackets                        => mv_suppressed_jackets.csv => datafile_suppressed_jackets
    # Manual Sleeve-Level Overrides to Snapshot => mv_kpi_overrides.csv      => datafile_manual_adjustments_to_sleeve
    # T/T1 Override                             => mv_tt1_overrides.csv      => datafile_tt1_difference
    
    my $uploadAllowed = &mvAumUploadIsAllowed($rhFormData, $CFG);
    unless(${$uploadAllowed}{'is_allowed'})
    {
        &mvAumLogger('Publisher::mvAumUploadFile. . .: ' . ${$uploadAllowed}{'message'});
        $rhData->{'MESSAGES'}->{'ERROR'} = ${$uploadAllowed}{'message'};
        return $rhData;
    }
    my $datafileCashAdjustmentsSleeve = 
        $rhFormData->{'datafile_cash_adjustments_sleeve'} || '';
    my $datafileGrossAumSleeve = 
        $rhFormData->{'datafile_gross_aum_sleeve'} || '';
    my $datafileManualAdjustmentsToSleeve = 
        $rhFormData->{'datafile_manual_adjustments_to_sleeve'} || '';
    my $datafileDoubleDip = 
        $rhFormData->{'datafile_double_dip'} || '';
    my $datafileSuppressedJackets = 
        $rhFormData->{'datafile_suppressed_jackets'} || '';
    my $datafileTT1Difference = 
        $rhFormData->{'datafile_tt1_difference'} || '';
    
    my @fileNames;
    my $fileCount = 0;
    my $errors = '';
    my $successes = '';
    # *********************************************************************************************
    # Manual Sleeve-Level Overrides to Snapshot => mv_kpi_overrides.csv => datafile_manual_adjustments_to_sleeve
    # *********************************************************************************************
    if($datafileManualAdjustmentsToSleeve)
    {
        $fileCount++;
        push @fileNames, $datafileManualAdjustmentsToSleeve;
        $rhFormData->{'this_datafile_item'} = 'datafile_manual_adjustments_to_sleeve';
        
        $rhData->{'MESSAGES'}->{'OK'} = '';
        $rhData->{'MESSAGES'}->{'ERROR'} = '';
        &mvAumUploadKpiFile($rhFormData, $CFG, $rhData);
        $successes .= $rhData->{'MESSAGES'}->{'OK'} || '';
        $errors .= $rhData->{'MESSAGES'}->{'ERROR'} || '';
    }
    # *********************************************************************************************
    # Double Dip => mv_dd_overrides.csv => datafile_double_dip
    # *********************************************************************************************
    if($datafileDoubleDip)
    {
        $fileCount++;
        push @fileNames, $datafileDoubleDip;
        $rhFormData->{'this_datafile_item'} = 'datafile_double_dip';
        
        $rhData->{'MESSAGES'}->{'OK'} = '';
        $rhData->{'MESSAGES'}->{'ERROR'} = '';
        
        &mvAumUploadDoubleDipFile($rhFormData, $CFG, $rhData);
        $successes .= $rhData->{'MESSAGES'}->{'OK'} || '';
        $errors .= $rhData->{'MESSAGES'}->{'ERROR'} || '';
    }
    # *********************************************************************************************
    # Suppressed Jackets => mv_suppressed_jackets.csv => datafile_suppressed_jackets
    # *********************************************************************************************
    if($datafileSuppressedJackets)
    {
        $fileCount++;
        push @fileNames, $datafileSuppressedJackets;
        $rhFormData->{'this_datafile_item'} = 'datafile_suppressed_jackets';
        
        $rhData->{'MESSAGES'}->{'OK'} = '';
        $rhData->{'MESSAGES'}->{'ERROR'} = '';
        
        &mvAumUploadSuppressionFile($rhFormData, $CFG, $rhData);
        $successes .= $rhData->{'MESSAGES'}->{'OK'} || '';
        $errors .= $rhData->{'MESSAGES'}->{'ERROR'} || '';
    }
    # *********************************************************************************************
    # T/T1 Override => mv_tt1_overrides.csv => datafile_tt1_difference
    # *********************************************************************************************
    if($datafileTT1Difference)
    {
        $fileCount++;
        push @fileNames, $datafileTT1Difference;
        $rhFormData->{'this_datafile_item'} = 'datafile_tt1_difference';
        
        $rhData->{'MESSAGES'}->{'OK'} = '';
        $rhData->{'MESSAGES'}->{'ERROR'} = '';
        
        &mvAumUploadTT1DifferenceFile($rhFormData, $CFG, $rhData);
        $successes .= $rhData->{'MESSAGES'}->{'OK'} || '';
        $errors .= $rhData->{'MESSAGES'}->{'ERROR'} || '';
    }
    
    if($errors)
    {
        $rhData->{'MESSAGES'}->{'ERROR'} = $errors;
    }
    else
    {
        $rhData->{'MESSAGES'}->{'OK'} = $successes;
    }
    if($fileCount == 0)
    {
        push @fileNames, 'file names MISSING';
    }
    
    return $rhData;
} # end sub sub mvAumUploadFile
=head2 C<mvAumDownloadOverrideData($rhFormData, $CFG)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumDownloadOverrideData
{
    my ($rhFormData,$CFG) = @_;
    my $rhData  = {};
    my $dbIdent = 'DSREAD';
    my $oPublisher = BFM::AUM::Publisher->new($rhData,$rhFormData,$CFG);
    
    my $information = 'Publisher::mvAumDownloadOverrideData. . .';
    #&mvAumLogger($information);
    
    # *********************************************************************************************
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    
    my $port_date = $rhFormData->{port_date};
    my $port_date_directory_name = &mvAumFormatDirectoryName($port_date);
    #my $uploadDirectory = BFM::GetFile::get_file('MV_UPLOAD_DIR');
    my $uploadDirectory = $MVUPLOAD_DIR;
    $uploadDirectory .= "/$port_date_directory_name/inbound";
    
    my $fileName = $BFM::AUM::MVAUM::KPI_OVERRIDE_FILE;
    my $fileNamePrefix = $fileName;
    $fileNamePrefix =~ s/.csv//;
    
    
    # my $sleeveFileNamePrefix = $KPI_FILE_NAME_PREFIX;
    #&mvAumLogger("Publisher::mvAumDownloadOverrideData. . .download: $sleeveFileNamePrefix.csv" );
    my $fileToDownload = "$uploadDirectory/$fileName";
    #&mvAumLogger("Publisher::mvAumDownloadOverrideData. . .fileToDownload: $fileToDownload" );
    
    my $webTempDir = '';
    my $from = '';
    my $to = '';
    my $message = '';
    
    my $fileIsOK = eval 
    {
        $webTempDir = BFM::GetFile::get_file('WebTempDir');
        $from = $fileToDownload;
        $to = "$webTempDir/$fileName";
        if(-e $fileToDownload)
        {
            copy($from, $to) or die "Cannot copy download file: $!\n";
            $rhData->{'open_report'} = "$fileName";
            $message = $user . ' has downloaded a KPI file on ' . $today;
            #&mvAumLogger($message);
        }
        else
        {
            #&mvAumLogger("Publisher::mvAumDownloadOverrideData. . .file does not exist: $user the KPI file does not exist on $today");
            die $user . ' the KPI file does not exist on ' . $today . "\n";
        }
        1;
    }; # end my $fileIsOK = eval {
    if($fileIsOK)
    {
        $rhData->{'MESSAGES'}->{'OK'} = $message;
    }
    else
    {
        $message = $@;
        #&mvAumLogger("####not fileIsOK: $message");
        $rhData->{'MESSAGES'}->{'ERROR'} = "$message";
    }
    
    # *********************************************************************************************
    
    return $rhData;
} # end sub mvAumDownloadOverrideData
=head2 C<mvAumDownloadJacketsData($rhFormData, $CFG)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumDownloadJacketsData
{
    my ($rhFormData,$CFG) = @_;
    my $rhData  = {};
    my $dbIdent = 'DSREAD';
    my $oPublisher = BFM::AUM::Publisher->new($rhData,$rhFormData,$CFG);
    
    my $information = 'Publisher::mvAumDownloadJacketsData. . .';
    #&mvAumLogger($information);
    
    # *********************************************************************************************
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    
    my $port_date = $rhFormData->{port_date};
    my $port_date_directory_name = &mvAumFormatDirectoryName($port_date);
    #my $uploadDirectory = BFM::GetFile::get_file('MV_UPLOAD_DIR');
    my $uploadDirectory = $MVUPLOAD_DIR;
    $uploadDirectory .= "/$port_date_directory_name/inbound";
    
    my $fileName = $BFM::AUM::MVAUM::SUPP_JACKETS_FILE;
    my $fileNamePrefix = $fileName;
    $fileNamePrefix =~ s/.csv//;
    
    my $sleeveFileNamePrefix = $fileNamePrefix;
    #&mvAumLogger("Publisher::mvAumDownloadJacketsData. . .download: $sleeveFileNamePrefix.csv" );
    my $fileToDownload = "$uploadDirectory/$sleeveFileNamePrefix.csv";
    #&mvAumLogger("Publisher::mvAumDownloadJacketsData. . .fileToDownload: $fileToDownload" );
    
    my $webTempDir = '';
    my $from = '';
    my $to = '';
    my $message = '';
    
    my $fileIsOK = eval 
    {
        $webTempDir = BFM::GetFile::get_file('WebTempDir');
        $from = $fileToDownload;
        $to = "$webTempDir/$sleeveFileNamePrefix.csv";
        if(-e $fileToDownload)
        {
            copy($from, $to) or die "Cannot copy download file: $!\n";
            $rhData->{'open_report'} = "$sleeveFileNamePrefix.csv";
            $message = $user . ' has downloaded a suppressed jackets file on ' . $today;
            #&mvAumLogger($message);
        }
        else
        {
            #&mvAumLogger("Publisher::mvAumDownloadJacketsData. . .file does not exist: $user the KPI file does not exist on $today");
            die $user . ' a suppressed jackets file does not exist on ' . $today . "\n";
        }
        1;
    }; # end my $fileIsOK = eval {
    if($fileIsOK)
    {
        $rhData->{'MESSAGES'}->{'OK'} = $message;
    }
    else
    {
        $message = $@;
        #&mvAumLogger("####not fileIsOK: $message");
        $rhData->{'MESSAGES'}->{'ERROR'} = "$message";
    }
    
    # *********************************************************************************************
    
    return $rhData;
} # end sub mvAumDownloadJacketsData
=head2 C<mvAumLogger($message)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumLogger
{
    my ($message) = @_;
    my $logLogNG = BFM::LogNG->new(
        File => {FileName => $MVDEBUG_LOG},
    );
    $logLogNG->Log(LOG_INFO,$message);
    
    return 1;
} # end sub mvAumLogger
=head2 C<mvAumGetOptions($rhFormData, $CFG)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumGetOptions
{
    my ($rhFormData, $CFG) = @_;
    my $rhData = {};
    my $oPublisher = BFM::AUM::Publisher->new($rhData, $rhFormData, $CFG);
    my $options = 
    {
        asof_date => $rhFormData->{'port_date'},
        purpose => $BFM::AUM::MVAUM::PURPOSE->{$oPublisher->{'snapshot_type'}},
    };
    my $oMvAum = BFM::AUM::MVAUM->new($options);
    my $isPublished = $oMvAum->is_published();
    my $status = $isPublished ? $BFM::AUM::MVAUM::STATUS->{PUBLISHED} : $BFM::AUM::MVAUM::STATUS->{DRAFT};
    my $user = $ENV{'REMOTE_USER'} || $ENV{'USER'} || $ENV{'LOGNAME'} || $ENV{'USERNAME'};
    
    $options = 
    {
        user => $user,
        asof_date => $rhFormData->{'port_date'},
        status => $status,
        purpose => $BFM::AUM::MVAUM::PURPOSE->{$oPublisher->{'snapshot_type'}},
    };
    
    return $options;
} # end sub mvAumGetOptions
=head2 C<mvAumGetOptionsWithMode($aum_date, $purpose, $user, $mode)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumGetOptionsWithMode
{
    my ($aum_date, $purpose, $user, $mode) = @_;
    my $options = 
    {
        asof_date => $aum_date,
        purpose => $purpose,
    };
    my $oMvAum = BFM::AUM::MVAUM->new($options);
    my $isPublished = $oMvAum->is_published();
    my $status = $isPublished ? $BFM::AUM::MVAUM::STATUS->{PUBLISHED} : $BFM::AUM::MVAUM::STATUS->{DRAFT};
    
    # rhoward 20150413
    $options = 
    {
        'mv_status' => $status,
        'mv_user' => $user,
        'aum_date' => $aum_date,
        'asof_date' => $aum_date,
        'mv_purpose' => $purpose,
        'mv_mode' => $mode,
    };
    my $dump = Dumper $options;
    &mvAumLogger("Publisher::mvAumGetOptionsWithMode. . .options: $dump");
    return $options;
} # end sub mvAumGetOptionsWithMode
=head2 C<mvAumGetMainData($rhFormData, $CFG)>
I<Description>
    Add description
I<Returns>
    Add return type
    
=cut
sub mvAumGetMainData
{
    my ($rhFormData, $CFG) = @_;
    # ensure period ending drop down appears
    my $rhData = &BFM::AUM::Publisher::getMainData($rhFormData, $CFG);
    # ensure server process/server state are passed to client
    my $rhDataMerged = {%{$rhData}, %{$rhFormData}};
    $rhData = $rhDataMerged;
    my $information = '';
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    
    # *********************************************************************************************
    
    my $oPublisher = BFM::AUM::Publisher->new($rhData, $rhFormData, $CFG);
    
    my $numberOfSnapshots = 0;
    
    my $blockIsOK = eval {
    
        my $options = &mvAumGetOptions($rhFormData, $CFG);
        $rhData->{'purpose'} = $options->{'purpose'};
        
        my $oMvAum = BFM::AUM::MVAUM->new($options);
        
        my $isPublished;
        
        my $results = $oMvAum->get_snapshots();
        $numberOfSnapshots = scalar(@$results);
            
        if($options->{'purpose'} eq $BFM::AUM::MVAUM::PURPOSE->{'MONTHLY'} || 
            $options->{'purpose'} eq $BFM::AUM::MVAUM::PURPOSE->{'WEEKLY'})
        {
            $isPublished = $oMvAum->is_published();
        
            $rhData->{'ENABLE_REFRESH'} = 1;
            if($isPublished )
            {
                $rhData->{'ENABLE_REFRESH'} = 0;
            }
            
            $rhData->{'ENABLE_CREATE'} = 1;
            if($isPublished)
            {
                $rhData->{'ENABLE_CREATE'} = 0;
            }
            $rhData->{'ENABLE_CREATE'} = 0;
            
            $rhData->{'ENABLE_PUBLISH'} = 1;
            if(!$isPublished && $numberOfSnapshots == 0)
            {
                $rhData->{'ENABLE_PUBLISH'} = 0;
            }
            if($isPublished && $numberOfSnapshots > 0)
            {
                $rhData->{'ENABLE_PUBLISH'} = 0;
            }
        }
        else
        {
            $rhData->{'ENABLE_REFRESH'} = 0;
            $rhData->{'ENABLE_CREATE'} = 0;
            $rhData->{'ENABLE_PUBLISH'} = 0;
        }
        
        1;
    };
    if($blockIsOK)
    {
        $information = "$numberOfSnapshots Snapshots found";
        if($numberOfSnapshots == 1)
        {
            $information = "$numberOfSnapshots Snapshot found";
        }
        if($rhData->{'MESSAGES'}->{'INFO'})
        {
            $information = $rhData->{'MESSAGES'}->{'INFO'};
        }
        $rhData->{'MESSAGES'}->{'OK'} = $information;
    }
    else
    {
        $information = "Initialization error: $@";
        &mvAumLogger($information);
        
        $information = "An error occurred";
        $rhData->{'MESSAGES'}->{'ERROR'} = $information;
    }
    # *********************************************************************************************
    
    return $rhData;
} # end sub mvAumGetMainData
=head2 C<mvAumFindBadPortfolioCodes(@listOfPortfolioCodes)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumFindBadPortfolioCodes
{
    my (@listOfPortfolioCodes) = @_;
    my @listOfBadPortfolioCodes = ();
    my $numberOfBadPortfolioCodes = 0;
    
    my $blockIsOK = eval
    {
        my $obj = DataObject->new();
        my $portfolios = $obj->GetTbl("portfolios");
        
        my $tempDBName = $obj->GetTempDBName();
        my $tempTable = 'temp_mv_table' . reverse(time);
        my $tempDBTable = "$tempDBName.guest.$tempTable";
        my $nonSharableTempTable = "#$tempTable";
        my $ds_write = BFM::get_file("DSWRITE");
        my $ds_read = BFM::get_file("DSREAD");
        
        $obj->SetDBIdent($ds_write);
        
        # create the table
        my $sql = qq[CREATE TABLE $tempDBTable (portfolio_code INT)];
        #print "$sql\n";
        $obj->DoSql($sql);
        
        
        foreach my $portfolioCode (@listOfPortfolioCodes)
        {
            $sql = "INSERT INTO $tempDBTable (portfolio_code) VALUES ($portfolioCode)";
            $obj->DoSql($sql);
            #print "INSERT INTO $tempDBTable (portfolio_code) VALUES ($portfolioCode)\n";
        }
    
$sql = qq[
SELECT t.portfolio_code 
FROM $portfolios AS p
RIGHT OUTER JOIN
$tempDBTable AS t
ON p.portfolio_code = t.portfolio_code
WHERE p.portfolio_code IS NULL
];
        my $recordCount = 0;
        my $portfolioCode = "";
        my $results = $obj->DoSql($sql);
        foreach my $row (@$results) 
        {
            $recordCount++;
            $portfolioCode = $row->{portfolio_code};
            push @listOfBadPortfolioCodes, $portfolioCode;
            #print "$portfolioCode\n";
        }
        
        my $dbh = $obj->GetDBHandle();
        $obj->disconnect() or warn $dbh->errstr;
        
        1;
    }; # my $blockIsOK = eval
    unless($blockIsOK)
    {
        # db exception did occur - returning all user submitted portfolio codes
        &mvAumLogger("Publisher::mvAumFindBadPortfolioCodes. . .exception occurred: $@");
        return @listOfPortfolioCodes;
    }
    else
    {
        # db exception did not occur - however there may be bad portfolio codes in the list
        $numberOfBadPortfolioCodes = @listOfBadPortfolioCodes;
        if($numberOfBadPortfolioCodes)
        {
            &mvAumLogger("Publisher::mvAumFindBadPortfolioCodes. . .error: $numberOfBadPortfolioCodes");
        }
        return @listOfBadPortfolioCodes;
    }
} # end sub mvAumFindBadPortfolioCodes
=head2 C<mvAumGetDifferenceReport($rhFormData, $CFG)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumGetDifferenceReport
{
    my ($rhFormData, $CFG) = @_;
    my $port_date = $rhFormData->{'port_date'};
    my $rhData = &BFM::AUM::Publisher::getMainData($rhFormData, $CFG);
    $rhData->{'port_date'} = $port_date;
    my $information = '';
    
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    my $todayFmt = $today->text_fmt($FMT_Slash);
    
    my $port_date_directory_name = &BFM::AUM::Publisher::mvAumFormatDirectoryName($port_date);
    my $directoryName = $BFM::AUM::MVAUM::MVBASE_DIR . '/' . 
        $port_date_directory_name . '/' .  
        $BFM::AUM::MVAUM::UPLOAD_DIR;
    my $latestFile = $directoryName . '/' . 
        $BFM::AUM::MVAUM::SUPP_JACKETS_FILE;
    my $previousFile = '';
    my $fileName = $BFM::AUM::MVAUM::SUPP_JACKETS_FILE;
    my $fileNamePrefix = $fileName;
    $fileNamePrefix =~ s/.csv//;
    my @fields = ();
    my $field = '';
    
    my @addedArray = ();
    my @deletedArray = ();
    
    my $blockIsOK = eval 
    {
        unless(-e $latestFile)
        {
            die "No suppressed jackets files exist at this time\n";
        }
        
        opendir(DIR, $directoryName);
        #my @files = grep(/$fileNamePrefix\_/, reverse sort{(stat $b)[9] <=> (stat $a)[9]} readdir(DIR));
        my @files = grep {/$fileNamePrefix/} reverse sort{(stat $b)[9] <=> (stat $a)[9]} readdir(DIR);
        closedir(DIR);
        
        my $numberOfFiles = @files;
        if($numberOfFiles <= 0)
        {
            # no files exist yet
            die "No difference files exist yet\n";
        }
        elsif($numberOfFiles == 1)
        {
            # no previous file - no difference
            die "No previous file for comparison exists yet\n";
        }
        else
        {
            # get previous file
            $previousFile = $directoryName . '/' . $files[1];
        }
        
        # read latest file
        # put latest portfolio codes into latest portfolio code array
        # close latest file
        # sort ascending latest portfolio code array
        my @latestFileContents = read_file($latestFile);
        my $latestFileContentsLength = @latestFileContents;
        my @latestArray = ();
        foreach my $latestFileLine(@latestFileContents)
        {
            @fields = split("\n", $latestFileLine);
            $field = $fields[0];
            push(@latestArray, $field);
        }
        @latestArray = sort @latestArray;
        
        
        # read previous file
        # put previous portfolio codes into previous portfolio code array
        # close previous file
        # sort ascending previous portfolio code array
        my @previousFileContents = read_file($previousFile);
        my $previousFileContentsLength = @previousFileContents;
        my @previousArray = ();
        foreach my $previousFileLine(@previousFileContents)
        {
            @fields = split("\n", $previousFileLine);
            $field = $fields[0];
            push(@previousArray, $field);
        }
        @previousArray = sort @previousArray;
        
        # scan through both latest array and previous array
        # push added items onto added array
        # push deleted items onto deleted array
        my $found = 0;
        
        $found = 0;
        for my $element (@latestArray)
        {
            for my $index (0..$#previousArray)
            {
                if($previousArray[$index] eq $element)
                {
                    $found = 1;
                    last;
                }
            }
            unless($found)
            {
                push(@addedArray, $element);
            }
            $found = 0;
        }
        $found = 0;
        for my $element (@previousArray)
        {
            for my $index (0..$#latestArray)
            {
                if($latestArray[$index] eq $element)
                {
                    $found = 1;
                    last;
                }
            }
            unless($found)
            {
                push(@deletedArray, $element);
            }
            $found = 0;
        }
        
        # format results
        # create .csv file with results
        # send email notice with .csv attached or provide link on interface to retrieve .csv file
        
        my $from = BFM::GetFile::get_file('MV_AUM_PUBLISHER_FROM');
        my $to = BFM::GetFile::get_file('MV_AUM_EMAIL_PUBLISH');
        my $subject = 'Difference Report';
        my $addedOutput   = join "\n", @addedArray;
        my $deletedOutput = join "\n", @deletedArray;
        my $message = 
            "\n$user has requested on $todayFmt a suppressed jackets difference report for portfolio date $port_date\n";
            
        if(scalar(@addedArray) || scalar(@deletedArray))
        {
            if(scalar(@addedArray))
            {
                $message .= "\n\nAdded Portfolios:\n\n$addedOutput";
            }
            if(scalar(@deletedArray))
            {
                $message .= "\n\nDeleted Portfolios:\n\n$deletedOutput";
            }
        }
        else
        {
            $message .= "\n\nAdded Portfolios:\n\nNone";
            $message .= "\n\nDeleted Portfolios:\n\nNone";
        }
        
        &mvAumSendEmail($rhFormData, $CFG, $from, $to, $subject, $message);
        
        1;
    };
    if($blockIsOK)
    {
        $information = 
            "\n$user has requested on $todayFmt a suppressed jackets difference report for portfolio date $port_date\n";
        #&mvAumLogger($information);
        $rhData->{'MESSAGES'}->{'OK'} = $information;
        $rhData->{'MESSAGES'}->{'INFO'} = $information;
    }
    else
    {
        $information = "$@";
        &mvAumLogger("$user has encountered an exception when generating a difference report on $todayFmt: $information");
        $rhData->{'MESSAGES'}->{'ERROR'} = "$information";
    }
    return $rhData;
} # end sub mvAumGetDifferenceReport
=head2 C<mvAumUploadTT1DifferenceFile($rhFormData, $CFG, $rhData)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumUploadTT1DifferenceFile
{
    my ($rhFormData, $CFG, $rhData) = @_;
    my $fieldName = $rhFormData->{'this_datafile_item'};
    my $inFH = $rhFormData->{$rhFormData->{'this_datafile_item'}};
    my $fileName = $BFM::AUM::MVAUM::TT1_OVERRIDE_FILE;
    my $fileNamePrefix = $fileName;
    $fileNamePrefix =~ s/.csv//;
    
    my $fmtInFH = &mvAumFormatFileName($inFH, $fileNamePrefix);
    
    my $recordCount = 0;
    my @contents;
    
    my $contentsLength;
    my @fields;
    my $field;
    my $message = '';
    my $mergeMessage = '';
    my $renameMessage = '';
    my $fileIsOK = 0;
    my $openIsOK = 0;
    my $sql;
    my @listOfPortfolioCodes = ();
    my @listOfBadPortfolioCodes = ();
    my $NUMBER_OF_TT1_COLUMNS = 2;
    my $DIRECTORY_PERMISSIONS = oct(777);
    
    my $blockIsOK = eval
    {
        my $port_date = $rhFormData->{port_date};
        my $port_date_directory_name = BFM::AUM::Publisher::mvAumFormatDirectoryName($port_date);
    
        if(!$fieldName)
        {
            $fileIsOK = 0;
            $message = "Publisher::mvAumUploadTT1DifferenceFile. . .file name is missing";
            die $message;
        }
        
        
        # *****************************************************************************************
        # *****************************************************************************************
        my $directoriesStatus = &mvAumBuildDirectories($port_date);
        unless(exists $directoriesStatus->{'MESSAGES'}->{'OK'})
        {
            $message = 'There was an error setting up the report directory. ' .
                $directoriesStatus->{'MESSAGES'}->{'ERROR'};
            &mvAumLogger($message);
            die("$message\n");
        }
        my $uploadDirectory = $directoriesStatus->{'MESSAGES'}->{'UPLOAD_DIRECTORY'};
        # *****************************************************************************************
        # *****************************************************************************************
        
        
        my $cgiTempDir = $CGITempFile::TMPDIRECTORY;
        $CGITempFile::TMPDIRECTORY = $uploadDirectory;
        my $cgi = CGI->new();
        
        my $file = $cgi->upload($fieldName); 
        my $tmpfilename = $cgi->tmpFileName($inFH);        
        
        $recordCount = 0;
        @contents = read_file($file);
        
        $contentsLength = @contents;
        $message = '';
        $fileIsOK = 0;
        
        $fileIsOK = 1;
        foreach my $line(@contents)
        {
            if($recordCount == 0)
            {
                # validate header record
                @fields = split(',', $line);
                if(scalar(@fields) != $NUMBER_OF_TT1_COLUMNS)
                {
                    $message = "TT1 file error: file must have $NUMBER_OF_TT1_COLUMNS columns for header. ";
                    $fileIsOK = 0;
                    last;
                }
                $field = $fields[0];
                if($field !~ m/portfolio_code/i)
                {
                    $message = "TT1 file error: must have a column called 'portfolio_code'. ";
                    $fileIsOK = 0;
                    last;
                }
                
                $field = $fields[1];
                if($field !~ m/type/i)
                {
                    $message = "TT1 file error: must have a column called 'type'. ";
                    $fileIsOK = 0;
                    last;
                }
            }
            else
            {
                # validate data records
                @fields = split(',', $line);
                if(scalar(@fields) != $NUMBER_OF_TT1_COLUMNS)
                {
                    $message = "TT1 file error: must have $NUMBER_OF_TT1_COLUMNS columns for data. ";
                    $fileIsOK = 0;
                    last;
                }
                $field = $fields[0];
                if($field !~ m/-?\d+/)
                {
                    $message = "TT1 file error: invalid portfolio code - $field. ";
                    $fileIsOK = 0;
                    last;
                }
                $field = $fields[1];
                if($field !~ m/(T|T1)/i)
                {
                    $message = "TT1 file error: invalid type code - $field. ";
                    $fileIsOK = 0;
                    last;
                }
            }
            $recordCount++;
        } # end foreach my $line(@contents)
        # assemble list of portfolio codes for sql statement
        if($fileIsOK)
        {
            $recordCount = 0;
            foreach my $line(@contents)
            {
                if($recordCount == 0)
                {
                    $recordCount++;
                    next;
                }
                
                
                @fields = split(',', $line);
                $field = $fields[0];
                
                push @listOfPortfolioCodes, $field;
                
                $recordCount++;
            } # end foreach my $line(@contents)
            
            # build and run sql statement to validate list of portfolio codes
            @listOfBadPortfolioCodes = &BFM::AUM::Publisher::mvAumFindBadPortfolioCodes(@listOfPortfolioCodes);
            
            $fileIsOK = 1;
            my $listOfBadPortfolioCodesLength = @listOfBadPortfolioCodes;
            if($listOfBadPortfolioCodesLength)
            {
                $fileIsOK = 0;
                $message = "TT1 file error: portfolio codes not found - @listOfBadPortfolioCodes";
            }
        } # end if($fileIsOK)
        
        if($fileIsOK)
        {
            open(my $FH, ">", "$uploadDirectory/$fmtInFH") or die("Publisher::mvAumUploadTT1DifferenceFile. . .not openIsOK: $!\n");
            foreach my $line(@contents)
            {
                # make sure in Unix format
                $line =~ s/\r$//;
                print $FH "$line";
            } # end foreach my $line(@contents)
            close $FH;
            
            # do overwrite
            $renameMessage = &mvAumUpdateStandardFile($uploadDirectory, $fileNamePrefix, $fmtInFH);
            if($renameMessage)
            {
                die $renameMessage;
            }
        }
        else
        {
            #&mvAumLogger("Publisher::mvAumUploadTT1DifferenceFile. . .message: $message");
            $rhData->{'MESSAGES'}->{'ERROR'} = $message;
        } # end if($fileIsOK)
        
        1;
    }; # end my $blockIsOK = eval
    
    if($blockIsOK)
    {
        # exception not thrown: something went right
        if($fileIsOK)
        {
            # &mvAumLogger("Publisher::mvAumUploadTT1DifferenceFile. . .: OK");
            $rhData->{'MESSAGES'}->{'OK'} = "TT1 difference file uploaded. ";
        }
        else
        {
            &mvAumLogger("TT1 difference file error: $message");
            $rhData->{'MESSAGES'}->{'ERROR'} = $message;
        }
    }
    else
    {
        $message = $@;
        # exception thrown: something went wrong
        &mvAumLogger("Publisher::mvAumUploadTT1DifferenceFile. . .: $message");
        $rhData->{'MESSAGES'}->{'ERROR'} = "TT1 difference file exception: $message. ";
    } # end if($blockIsOK)
    return $rhData;
} # end sub mvAumUploadTT1DifferenceFile
=head2 C<mvAumDownloadTT1DifferenceData($rhFormData, $CFG)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumDownloadTT1DifferenceData
{
    my ($rhFormData,$CFG) = @_;
    my $rhData  = {};
    my $dbIdent = 'DSREAD';
    my $oPublisher = BFM::AUM::Publisher->new($rhData,$rhFormData,$CFG);
    
    my $information = 'Publisher::mvAumDownloadTT1DifferenceData. . .';
    # *********************************************************************************************
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    
    my $port_date = $rhFormData->{port_date};
    my $port_date_directory_name = &mvAumFormatDirectoryName($port_date);
    #my $uploadDirectory = BFM::GetFile::get_file('MV_UPLOAD_DIR');
    my $uploadDirectory = $MVUPLOAD_DIR;
    $uploadDirectory .= "/$port_date_directory_name/inbound";
    
    my $fileName = $BFM::AUM::MVAUM::TT1_OVERRIDE_FILE;
    my $fileNamePrefix = $fileName;
    $fileNamePrefix =~ s/.csv//;
    
    my $fileToDownload = "$uploadDirectory/$fileName";
    
    my $webTempDir = '';
    my $from = '';
    my $to = '';
    my $message = '';
    
    my $fileIsOK = eval 
    {
        $webTempDir = BFM::GetFile::get_file('WebTempDir');
        $from = $fileToDownload;
        $to = "$webTempDir/$fileName";
        
        if(-e $fileToDownload)
        {
            copy($from, $to) or die "Cannot copy download file: $!\n";
            $rhData->{'open_report'} = "$fileName";
            $message = $user . ' has downloaded a TT1 difference file on ' . $today;
        }
        else
        {
            die $user . ' the TT1 difference file does not exist on ' . $today . "\n";
        }
        1;
    }; # end my $fileIsOK = eval {
    if($fileIsOK)
    {
        $rhData->{'MESSAGES'}->{'OK'} = $message;
    }
    else
    {
        $message = $@;
        #&mvAumLogger("####not fileIsOK: $message");
        $rhData->{'MESSAGES'}->{'ERROR'} = "$message";
    }
    
    # *********************************************************************************************
    
    return $rhData;
} # end sub mvAumDownloadTT1DifferenceData
=head2 C<mvAumBuildDirectories($port_date)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumBuildDirectories
{
    my ($port_date) = @_;
    my $rhData ={};
    my $message = '';
    my $uploadDirectory = $MVUPLOAD_DIR;
    
    my $blockIsOK = eval
    {
        my $DIRECTORY_PERMISSIONS = oct(777);
        
        my $port_date_directory_name = BFM::AUM::Publisher::mvAumFormatDirectoryName($port_date);
        $uploadDirectory .= "/$port_date_directory_name";
        my $inboundDirectory = $uploadDirectory . '/' . $BFM::AUM::MVAUM::UPLOAD_DIR;
        my $reportsDirectory = $uploadDirectory . '/' . $BFM::AUM::MVReports::REPORT_DIR;
        
        umask oct(000);
        unless(-e $uploadDirectory)
        {
            mkdir($uploadDirectory, $DIRECTORY_PERMISSIONS) 
                or die "Upload directory could not be created: $!\n";
        }
        unless(-e $inboundDirectory)
        {
            mkdir($inboundDirectory, $DIRECTORY_PERMISSIONS)
                or die "Inbound directory could not be created: $!\n";
        }
        unless(-e $reportsDirectory)
        {
            mkdir($reportsDirectory, $DIRECTORY_PERMISSIONS)
                or die "Reports directory could not be created: $!\n";
        }
        $uploadDirectory .= '/' . $BFM::AUM::MVAUM::UPLOAD_DIR;
        
        1;
    };
    if($blockIsOK)
    {
        $message = "Directories built properly.";
        # no exception thrown: something went right
        $rhData->{'MESSAGES'}->{'OK'} = $message;
        $rhData->{'MESSAGES'}->{'INFO'} = $message;
        $rhData->{'MESSAGES'}->{'UPLOAD_DIRECTORY'} = $uploadDirectory;
    }
    else
    {
        $message = $@;
        # exception thrown: something went wrong
        &mvAumLogger("Publisher::mvAumBuildDirectories. . .: $message");
        $rhData->{'MESSAGES'}->{'ERROR'} = "Build directories exception: $message";
    }
    return $rhData;
} # end sub mvAumBuildDirectories
=head2 C<mvAumGenerateReports($rhFormData, $CFG)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumGenerateReports
{
    my ($rhFormData, $CFG) = @_;
    my $port_date = $rhFormData->{'port_date'};
    my $rhData = &BFM::AUM::Publisher::getMainData($rhFormData, $CFG);
    $rhData->{'port_date'} = $port_date;
    
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    my $message = '';
    my $blockIsOK = eval 
    {
        my $oPublisher = BFM::AUM::Publisher->new($rhData, $rhFormData, $CFG);
        
        # *****************************************************************************************
        # *****************************************************************************************
        my $directoriesStatus = &mvAumBuildDirectories($rhFormData->{'data-snapshot-date'});
        unless(exists $directoriesStatus->{'MESSAGES'}->{'OK'})
        {
            $message = 'There was an error setting up the report directory. ' . 
                $directoriesStatus->{'MESSAGES'}->{'ERROR'};
            &mvAumLogger($message);
            die("$message\n");
        }
        # *****************************************************************************************
        # *****************************************************************************************
        
        # *****************************************************************************************
        # *****************************************************************************************
        my $datatype = $rhFormData->{'data-datatype'} || $BFM::AUM::MVAUM::PURPOSE->{$oPublisher->{'snapshot_type'}}; 
        my $asof_date = $rhFormData->{'data-snapshot-date'} || $port_date;
        
        my $mode = 'generate_reports';
        my $options = &mvAumGetOptionsWithMode($asof_date, $datatype, $user, $mode);
        # *****************************************************************************************
        # *****************************************************************************************
        
        # *****************************************************************************************
        # *****************************************************************************************
        my $oDispatcher = BFM::MIS::MisDispatcher->new();
        my $writeFileOK = $oDispatcher->writeFile('MVGeneric', $options);
        unless($writeFileOK) 
        {
            die($oDispatcher->{'ERROR'} . "\n");
        }
        # *****************************************************************************************
        # *****************************************************************************************
        
        1;
    };  # end my $blockIsOK = eval
    if($blockIsOK)
    {
        $message = 
            "$user has scheduled generation of reports on $today. An email will be sent upon completion. ";
        $rhData->{'MESSAGES'}->{'OK'} = $message;
        $rhData->{'MESSAGES'}->{'INFO'} = $message;
    }
    else
    {
        $message = $@;
        &mvAumLogger($message);
        $rhData->{'MESSAGES'}->{'ERROR'} = $message;
    }
    
    return $rhData;
} # end sub mvAumGenerateReports
=head2 C<mvAumOpenCrossHoldings($rhFormData, $CFG, $rhData)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumOpenCrossHoldings
{
    my ($rhFormData, $CFG, $rhData) = @_;
    $rhData = $rhFormData;
    my $fileName = $BFM::AUM::MVReports::HOLDINGS_FILE;
    
    my $rhMessage = &mvAumOpenExcelReports($rhFormData, $CFG, $rhData, $fileName);
    
    if($rhMessage->{'MESSAGES'}->{'OK'})
    {
        $rhData->{'MESSAGES'}->{'OK'} = $rhMessage->{'MESSAGES'}->{'OK'};
        $rhData->{'MESSAGES'}->{'INFO'} = $rhMessage->{'MESSAGES'}->{'OK'};
    }
    else
    {
        $rhData->{'MESSAGES'}->{'ERROR'} = $rhMessage->{'MESSAGES'}->{'ERROR'};
    }
    return $rhData;
} # end sub mvAumOpenCrossHoldings
=head2 C<mvAumOpenWalkforward($rhFormData, $CFG, $rhData)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
    
=cut
sub mvAumOpenWalkforward
{
    my ($rhFormData, $CFG, $rhData) = @_;
    $rhData = $rhFormData;
    my $fileName = $BFM::AUM::MVReports::WALKFORWARD_FILE;
    
    my $rhMessage = &mvAumOpenExcelReports($rhFormData, $CFG, $rhData, $fileName);
    
    if($rhMessage->{'MESSAGES'}->{'OK'})
    {
        $rhData->{'MESSAGES'}->{'OK'} = $rhMessage->{'MESSAGES'}->{'OK'};
        $rhData->{'MESSAGES'}->{'INFO'} = $rhMessage->{'MESSAGES'}->{'OK'};
    }
    else
    {
        $rhData->{'MESSAGES'}->{'ERROR'} = $rhMessage->{'MESSAGES'}->{'ERROR'};
    }
    return $rhData;
} # end sub mvAumOpenWalkforward
    
=head2 C<mvAumUploadIsAllowed($rhFormData, $CFG)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
=cut
sub mvAumUploadIsAllowed
{
    my ($rhFormData, $CFG) = @_;
    
    my $options = &mvAumGetOptions($rhFormData, $CFG);
    
    my $oMvAum = BFM::AUM::MVAUM->new($options);
    my $result = {'is_allowed' => 1,
        'message' => q{},
    };
    
    if($oMvAum->is_published())
    {
        &mvAumLogger('Publisher::mvAumUploadIsAllowed. . .is published');
        ${$result}{'is_allowed'} = 0;
        ${$result}{'message'} = "The snapshot is already published and no further uploads are allowed.";
    }
    return $result;
} # end sub mvAumUploadIsAllowed
=head2 C<mvAumGetUser()>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
=cut
sub mvAumGetUser
{
    my $user = $ENV{'REMOTE_USER'} || $ENV{'USER'};
    return $user;
}
=head2 C<mvAumDownloadKpiData($rhFormData, $CFG)>
I<Description>
    Add description
I<Arguments>
    Add arguments
I<Returns>
    Add return type
=cut
sub mvAumDownloadKpiData
{
    my ($rhFormData,$CFG) = @_;
    my $rhData  = {};
    my $port_date = $rhFormData->{port_date};
    my $oPublisher = BFM::AUM::Publisher->new($rhData, $rhFormData, $CFG);
    my $datatype = $BFM::AUM::MVAUM::PURPOSE->{$oPublisher->{'snapshot_type'}};
    
    &mvAumLogger("BFM::AUM::MVPublisher::mvAumDownloadKpiData: $port_date");
    &mvAumLogger("BFM::AUM::MVPublisher::mvAumDownloadKpiData: $datatype");
    
    my $user = &mvAumGetUser();
    my $today = BFMDate->new();
    my $message = '';
    my $blockIsOK = eval 
    {
        my $dobj = DataObject->new();
        my $aum_snapshot = $dobj->GetTbl("aum_snapshot");
        my $sql = qq[
SELECT portfolio_code, 
'' AS 'portfolio_name', 
gross_aum,
aum_dd, 
(flowsin_dd+flowsout_dd) AS 'aum_adjdd',
adjin,
adjout,
cashin,
cashout,
assetin,
assetout,
transfer_in,
transfer_out,
acquisition,
disposition
FROM $aum_snapshot
WHERE aum_date = '$port_date' 
AND datatype = '$datatype'
        ];
        &mvAumLogger("BFM::AUM::MVPublisher::mvAumDownloadKpiData: $sql");
        my $dbIdent = BFM::GetFile::get_file('DSREAD');
        $dobj->SetDBIdent($dbIdent);
        my $results = $dobj->DoSql($sql);
        my $output = '';
        
        my $portfolio_code = '';
        my $portfolio_name = '';
        my $gross_aum = '';
        my $aum_dd = '';
        my $aum_adjdd = '';
        my $adjin = '';
        my $adjout = '';
        my $cashin = '';
        my $cashout = '';
        my $assetin = '';
        my $assetout = '';
        my $transfer_in = '';
        my $transfer_out = '';
        my $acquisition = '';
        my $disposition = '';
        
        $output = "PORTFOLIO_CODE,PORTFOLIO_NAME,GROSS_AUM,AUM_DD,AUM_ADJDD,ADJIN,ADJOUT,CASHIN,CASHOUT,ASSETIN,ASSETOUT,TRANSFER_IN,TRANSFER_OUT,ACQUISITION,DISPOSITION\n";
        foreach my $row (@$results)
        {
            $portfolio_code = $row->{'portfolio_code'};
            $portfolio_name = $row->{'portfolio_name'};
            $gross_aum = $row->{'gross_aum'};
            $aum_dd = $row->{'aum_dd'};
            $aum_adjdd = $row->{'aum_adjdd'};
            $adjin = $row->{'adjin'};
            $adjout = $row->{'adjout'};
            $cashin = $row->{'cashin'};
            $cashout = $row->{'cashout'};
            $assetin = $row->{'assetin'};
            $assetout = $row->{'assetout'};
            $transfer_in = $row->{'transfer_in'};
            $transfer_out = $row->{'transfer_out'};
            $acquisition = $row->{'acquisition'};
            $disposition = $row->{'disposition'};
            $output .= "$portfolio_code,$portfolio_name,$gross_aum,$aum_dd,$aum_adjdd,$adjin,$adjout,$cashin,$cashout,$assetin,$assetout,$transfer_in,$transfer_out,$acquisition,$disposition\n";
        }
        my $webTempDir = BFM::GetFile::get_file('WebTempDir');
        my $fileName = 'kpi_download_file.csv';
        my $from = '';
        my $to = "$webTempDir/$fileName";
        &mvAumLogger("BFM::AUM::MVPublisher::mvAumDownloadKpiData: $to");
        open(my $FH, '>', $to) or die("File write error: $!");
        print $FH $output;
        close($FH);
        $rhData->{'open_report'} = "$fileName";
        
        1;
    }; # end my $blockIsOK = eval {
    if($blockIsOK)
    {
        $message = $user . ' has downloaded a KPI file on ' . $today;
        $rhData->{'MESSAGES'}->{'OK'} = $message;
    }
    else
    {
        $message = $@;
        &mvAumLogger("BFM::AUM::MVPublisher::mvAumDownloadKpiData: $message");
        $rhData->{'MESSAGES'}->{'ERROR'} = "$message";
    }
    
    return $rhData;
} # end sub mvAumDownloadKpiData
# *************************************************************************************************
# end mv aum
# *************************************************************************************************
1;
__END__
=head1 AUTHOR
rhoward@blackrock.com
=cut
