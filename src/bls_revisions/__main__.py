'''Unified CLI for bls-revisions: release, download, process.'''

import sys


def cmd_release() -> None:
    '''Scrape BLS news releases and build release_dates + vintage_dates parquets.'''
    from bls_revisions.release_dates.__main__ import main as release_main
    release_main()


def cmd_download() -> None:
    '''Download CES and QCEW data files.'''
    from pathlib import Path
    from bls_revisions.download import download_ces, download_qcew

    data_dir = Path.cwd() / 'data'
    print('Downloading CES...')
    download_ces(data_dir=data_dir)
    print('Downloading QCEW...')
    download_qcew(data_dir=data_dir)
    print('Done.')


def cmd_process() -> None:
    '''Run all processing steps: CES national, CES states, QCEW, vintage series.'''
    from bls_revisions.processing.ces_national import main as ces_national_main
    from bls_revisions.processing.ces_states import main as ces_states_main
    from bls_revisions.processing.qcew import main as qcew_main
    from bls_revisions.processing.vintage_series import main as vintage_series_main

    ces_national_main()
    ces_states_main()
    qcew_main()
    vintage_series_main()


def main() -> None:
    '''Entry point: dispatch to subcommand or run all steps.'''
    args = sys.argv[1:]

    if not args:
        cmd_release()
        cmd_download()
        cmd_process()
        return

    subcommand = args[0]
    if subcommand == 'release':
        cmd_release()
    elif subcommand == 'download':
        cmd_download()
    elif subcommand == 'process':
        cmd_process()
    else:
        print(f'Unknown subcommand: {subcommand}')
        print('Usage: bls-revisions [release|download|process]')
        sys.exit(1)


if __name__ == '__main__':
    main()
