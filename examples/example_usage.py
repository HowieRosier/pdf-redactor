import os
from redactor import BatchRedactionManager

# --- CONFIGURATION ---
PDF_SOURCE_FOLDER = 'pdf_source'
XML_OUTPUT_FOLDER = 'grobid_raw_xml_output'
REDACTED_PDF_FOLDER = 'redacted_pdf_output'

# URL of the running GROBID service
GROBID_SERVICE_URL = "http://localhost:8070/api/processFulltextDocument"

# Number of parallel threads to use
WORKER_THREADS = 8


def setup_test_environment():
    """
    Creates the necessary folders and adds a placeholder PDF
    for testing purposes.
    """
    print("Setting up test environment...")
    for folder in [PDF_SOURCE_FOLDER, XML_OUTPUT_FOLDER, REDACTED_PDF_FOLDER]:
        os.makedirs(folder, exist_ok=True)

    # Create a dummy file so the script has something to process.
    # In a real scenario, your teammate would add their own PDFs here.
    dummy_pdf_path = os.path.join(PDF_SOURCE_FOLDER, 'dummy_placeholder.pdf')
    if not os.path.exists(dummy_pdf_path):
        with open(dummy_pdf_path, 'w') as f:
            f.write("This is not a real PDF. Please replace with actual PDF files.")
        print(f"Created a placeholder file at '{dummy_pdf_path}'.")
        print("--> Please add your PDF files to the 'pdf_source' directory to run a real test.")


def run_redaction_process():
    """
    This is the main function showing how to use the BatchRedactionManager.
    """
    print("\nInitializing the Batch Redaction Manager...")

    # 1. Instantiate the manager with your configuration
    manager = BatchRedactionManager(
        pdf_dir=PDF_SOURCE_FOLDER,
        xml_dir=XML_OUTPUT_FOLDER,
        redacted_dir=REDACTED_PDF_FOLDER,
        grobid_url=GROBID_SERVICE_URL,
        workers=WORKER_THREADS
    )

    # 2. Run the full process
    print("Starting the redaction process...")
    manager.run()
    print("\nTest script finished.")


if __name__ == "__main__":
    setup_test_environment()
    # To run a real test, the user should have PDFs in the source folder
    # and a running GROBID service.
    run_redaction_process()