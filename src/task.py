

import argparse
import logging
from AFQ.data import fetch_hcp
import AFQ.api as api
import AFQ.mask as afm
import s3fs


def afq_hcp(subject,
            # shell, session, seg_algo, reuse_tractography,
            # use_callosal,
            aws_access_key, aws_secret_key, hcp_aws_access_key,
            hcp_aws_secret_key, outbucket):

    # Configuration:
    seg_algo = "afq"

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__) # noqa

    fs = s3fs.S3FileSystem(aws_access_key, aws_secret_key)

    # get HCP data for the given subject / session
    _, hcp_bids = fetch_hcp(
        [subject],
        profile_name=False,
        aws_access_key_id=hcp_aws_access_key,
        aws_secret_access_key=hcp_aws_secret_key)

    # if use_callosal, use the callosal bundles
    # if use_callosal:
    #     bundle_info = api.BUNDLES + api.CALLOSUM_BUNDLES
    # else:
    bundle_info = None

    # if single shell, only use b values between 990 and 1010
    # if "single" in shell.lower():
    #     tracking_params = {"odf_model": "DTI"}
    #     kwargs = {
    #         "min_bval": 990,
    #         "max_bval": 1010
    #     }
    # # if multi shell, use DKI instead of CSD everywhere
    # else:

    tracking_params = {
        'seed_mask': afm.ScalarMask('dki_fa'),
        'stop_mask': afm.ScalarMask('dki_fa'),
        "odf_model": "DKI"}
    kwargs = {
        "scalars": ["dki_fa", "dki_md"]
    }

    # use csd if csd is in shell
    # if "csd" in shell.lower():
    tracking_params["odf_model"] = "CSD"

    # Whether to reuse a previous tractography that has already been uploaded to
    # s3 by another run of this function. Useful if you want to try new
    # parameters that do not change the tractography.
    # if reuse_tractography:
    #     fs.get(
    #         (
    #             f"{my_hcp_key}/{shell}_shell/"
    #             f"hcp_{session.lower()}_afq/sub-{subject}/ses-01/"
    #             f"sub-{subject}_dwi_space-RASMM_model-"
    #             f"{tracking_params['odf_model']}_desc-det_tractography.trk"),
    #         op.join(hcp_bids, f"derivatives/dmriprep/sub-{subject}/ses-01/sub-{subject}_customtrk.trk"))
    #     custom_tractography_bids_filters = {
    #         "suffix": "customtrk", "scope": "dmriprep"}
    # else:
    custom_tractography_bids_filters = None

    # Initialize the AFQ object with all of the parameters we have set so far
    # Also uses the brain mask provided by HCP
    # Sets viz_backend='plotly' to make GIFs in addition to the default
    # html visualizations (this adds ~45 minutes)
    myafq = api.AFQ(
        hcp_bids,
        brain_mask=afm.LabelledMaskFile(
                    'seg', {'scope': 'dmriprep'}, exclusive_labels=[0]),
        custom_tractography_bids_filters=custom_tractography_bids_filters,
        tracking_params=tracking_params,
        bundle_info=bundle_info,
        segmentation_params={"seg_algo": seg_algo, "reg_algo": "syn"},
        viz_backend='plotly',
        **kwargs)
    # run the AFQ objects
    myafq.export_all()

    # upload the results to outbucket, organized by parameters used
    remote_export_path =\
        "%s/hcp_1200_%s" % (outbucket, seg_algo)
    # if use_callosal:
    #     remote_export_path = remote_export_path + "_callosal"
    myafq.upload_to_s3(fs, remote_export_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--subject', type=int, required=True,
                        help='subject ID in the HCP dataset')
    parser.add_argument('--ak', type=str, required=True,
                        help='AWS Access Key')
    parser.add_argument('--sk', type=str, required=True,
                        help='AWS Secret Key')
    parser.add_argument('--hcpak', type=str, required=True,
                        help='AWS Access Key for HCP dataset')
    parser.add_argument('--hcpsk', type=str, required=True,
                        help='AWS Secret Key for HCP dataset')
    parser.add_argument('--outbucket', type=str, required=True,
                        help='Where do I put the outputs')

    args = parser.parse_args()
    afq_hcp(args.subject,
            args.ak, args.sk,
            args.hcpak, args.hcpsk,
            args.outbucket)
