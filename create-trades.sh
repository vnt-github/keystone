CODE_DIR=$0
STOCKS_DATA_DIR=$PWD
HTML_FORMAT=$1
MONTH_1_DIR=$2
MONTH_2_DIR=$3
TMP_DIR=$4

if [[ $HTML_FORMAT = 2001 || $HTML_FORMAT = 2016 ]] ; then
    echo -e "\nHTML FORMAT NOT SUPPORTED:" $HTML_FORMAT "stocks data profiles doesn't have enough inforamation like quarterly values of various field which are needed to make f,g and magic_score values. \n" 1>&2
    exit 1
fi

if [ $# -lt 4  ] ; then
    echo "usage: create-trades.sh html_profiles month_1 month_1 tmp_dir_path" 1>&2
    exit 1
fi

MONTH_1=$(basename $MONTH_1_DIR)
MONTH_2=$(basename $MONTH_2_DIR)

cd ${CODE_DIR%create-trades.sh}/scraper
source ${CODE_DIR%create-trades.sh}/venvs/tutorials/bin/activate
 
CRAWS_RES=$(scrapy crawl profiles_$HTML_FORMAT -a is_testing=True -a html_format=$HTML_FORMAT -a month_1=$MONTH_1 -a month_2=$MONTH_2 -a tmp_dir=$TMP_DIR -a stocks_data_dir=$STOCKS_DATA_DIR --overwrite-output=$TMP_DIR/stocks_data_$HTML_FORMAT-$MONTH_2.jsonlines --loglevel=ERROR)
if $CRAWS_RES ; then
    cd ../
    python trade.py $HTML_FORMAT $MONTH_1 $MONTH_2 $TMP_DIR $STOCKS_DATA_DIR
fi

cd $STOCKS_DATA_DIR