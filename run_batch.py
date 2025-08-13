import argparse
from redactor import BatchRedactionManager

# --- Default Configuration ---
DEFAULT_PDF_SOURCE_DIR = 'pdf_source'
DEFAULT_XML_OUTPUT_DIR = 'grobid_raw_xml_output'
DEFAULT_REDACTED_PDF_DIR = 'redacted_pdf_output'

def main():
    """
    Parses command-line arguments and runs the batch manager.
    """
    parser = argparse.ArgumentParser(description="A full-cycle PDF reference redactor using GROBID.")
    parser.add_argument("--pdf_dir", type=str, default=DEFAULT_PDF_SOURCE_DIR, help="Directory for source PDF files.")
    parser.add_argument("--xml_dir", type=str, default=DEFAULT_XML_OUTPUT_DIR,
                        help="Directory to store intermediate GROBID XML files.")
    parser.add_argument("--output_dir", type=str, default=DEFAULT_REDACTED_PDF_DIR,
                        help="Directory to save final, redacted PDF files.")
    parser.add_argument("--url", type=str, default="http://localhost:8070/api/processFulltextDocument",
                           help="GROBID service URL.")
    parser.add_argument("--workers", type=int, default=10,
                        help="Number of concurrent workers for both XML generation and PDF redaction.")

    args = parser.parse_args()

    manager = BatchRedactionManager(
        pdf_dir=args.pdf_dir,
        xml_dir=args.xml_dir,
        redacted_dir=args.output_dir,
        grobid_url=args.url,
        workers=args.workers
    )
    manager.run()


if __name__ == "__main__":
    main()