nextflow.enable.dsl=2

include { convert_BCF2VCF_BCFtools } from "../external/pipeline-Nextflow-module/modules/BCFtools/convert_BCF2VCF_BCFtools/main.nf"

include { compress_index_VCF } from "../external/pipeline-Nextflow-module/modules/common/index_VCF_tabix/main.nf"

workflow convert_BCF2VCF {
    take:
    sample_name
    variant_file
    variant_file_index

    main:

    convert_meta = Channel.value([
        "docker_image": params.docker_image_bcftools,
        "log_output_dir": "${params.log_output_dir}",
        "output_dir": "${params.output_dir_base}/DELLY-${params.delly_version}/output"
    ])

    compress_meta = Channel.value([
        "output_dir": "${params.output_dir_base}/DELLY-${params.delly_version}",
        "log_output_dir": "${params.log_output_dir}/process-log",
        "save_intermediate_files": params.save_intermediate_files
    ])

    bcf2vcf_channel = convert_meta.combine(sample_name)
        .map{ inter_conv_meta -> inter_conv_meta[0] + ["id": inter_conv_meta[1]] }
        .combine(variant_file)
        .combine(variant_file_index)

    convert_BCF2VCF_BCFtools(bcf2vcf_channel)

    index_channel = compress_meta.combine(sample_name)
        .map{ inter_compress_meta -> inter_compress_meta[0] + ["id": inter_compress_meta[1]] }
        .combine(convert_BCF2VCF_BCFtools.out.vcf)

    compress_index_VCF(index_channel)

    emit:
    gzvcf = compress_index_VCF.out.index_out.map{ it -> ["${it[1]}"] }
    idx = compress_index_VCF.out.index_out.map{ it -> ["${it[2]}"] }
}
