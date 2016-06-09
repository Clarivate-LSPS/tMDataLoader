package com.thomsonreuters.lsps.transmart.etl

/**
 *  Replace. Completely reload the data for the data types (clinical, expression, SNP) we are loading (like it works now). As in deleting that branch and loading it again.
 *  Update. Same as replace, but instead of deleting all data for the given datatype, just delete data for the subjects we are uploading.
 *  Append. Replace existing values for any given subject-visit-category combination and add new if they don’t exist
 *  Update variables. Same as append, but change categorical variables.
 */
enum MergeMode {
    REPLACE, UPDATE, APPEND, UPDATE_VARIABLES
}