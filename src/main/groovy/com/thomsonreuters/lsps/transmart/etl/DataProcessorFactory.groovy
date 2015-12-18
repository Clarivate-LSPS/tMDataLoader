package com.thomsonreuters.lsps.transmart.etl

class DataProcessorFactory {

    private static Map dataProcessors = [
            Expression: ExpressionDataProcessor,
            SNP       : SNPDataProcessor,
            VCF       : VCFDataProcessor,
            RBM       : RBMDataProcessor,
            Meta      : MetaDataProcessor,
            Clinical  : ClinicalDataProcessor,
            MIRNA_SEQ   : MIRNADataProcessor,
            MIRNA_QPCR  : MIRNADataProcessor,
            Protein     : ProteinDataProcessor,
            Metabolomics: MetabolomicsDataProcessor,
            RNASeq      : RNASeqDataProcessor,
            SerialHDD   : SerialHDDDataProcessor,
            ACGH        : ACGHDataProcessor
    ]

    static Set<String> getProcessorsType() {
        return dataProcessors.keySet()
    }

    static DataProcessor newDataProcessor(String type, config) {
        dataProcessors[type].newInstance(config)
    }


}
