nextflow.enable.dsl=2

include { call_SV_SVision } from "./svision.nf"

include { compress_index_VCF } from "../external/pipeline-Nextflow-module/modules/common/index_VCF_tabix/main.nf"

include { generate_checksum_PipeVal as generate_sha512_SVision } from "../external/pipeline-Nextflow-module/modules/PipeVal/generate-checksum/main.nf"

workflow workflow_SVision {
    take:
    META
    bams_to_call

    main:
    call_SV_SVision(
        META,
        bams_to_call,
        params.svision_cnn_model,
        params.reference_fasta,
        "${params.reference_fasta}.fai"
    )

    compress_meta = META.map{ base_m ->
        base_m + [
            "output_dir": "${base_m.workflow_output_dir}",
            "log_output_dir": "${params.log_output_dir}/process-log",
            "save_intermediate_files": params.save_intermediate_files
        ]
    }

    compress_index_VCF(
        compress_meta.combine(call_SV_SVision.out.vcf)
            .map{ compress_m -> [compress_m[0] + ["id": compress_m[1]], compress_m[2]] }
    )

    compress_index_VCF.out.index_out.map{ it -> ["${it[1]}"] }
        .mix(compress_index_VCF.out.index_out.map{ it -> ["${it[2]}"] })
        .set{ files_for_checksum }

    checksum_meta = META.map{ base_m ->
        base_m + [
            "output_dir": "${base_m.workflow_output_dir}/output",
            "docker_image": params.docker_image_validate
        ]
    }

    generate_sha512_SVision(checksum_meta.combine(files_for_checksum))
}
