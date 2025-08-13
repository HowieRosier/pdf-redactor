import os
import shutil
import statistics
import xml.etree.ElementTree as ET
from collections import defaultdict
import pymupdf as fitz
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Constants for ReferenceRedactor ---
HORIZONTAL_THRESHOLD_RATIO = 0.16
LINE_HEIGHT_MULTIPLIER = 4.0
VERTICAL_Y_THRESHOLD_TOP = 120.0
VERTICAL_Y_THRESHOLD_BOTTOM = 80.0
DELETION_APPEARANCE = 'whiteout'
DELETION_PADDING = 7


# ==============================================================================
#  WORKER CLASS: Processes a single PDF file
# ==============================================================================

class ReferenceRedactor:
    """
    Encapsulates the process of identifying and covering reference areas
    for a single PDF file using a corresponding GROBID XML.
    """
    def __init__(self, xml_path: str, pdf_path: str, output_pdf_path: str):
        self.xml_path = xml_path
        self.pdf_path = pdf_path
        self.output_pdf_path = output_pdf_path
        self.final_pymupdf_coords = []

    def process(self) -> tuple[bool, str]:
        if not os.path.exists(self.pdf_path):
            return False, f"Corresponding PDF not found at '{self.pdf_path}'"
        self._calculate_final_coordinates()
        if not self.final_pymupdf_coords:
            shutil.copy(self.pdf_path, self.output_pdf_path)
            return True, "No references found; original file copied."
        return self._draw_rects_and_save()

    def _calculate_final_coordinates(self):
        boxes_by_page = self._extract_raw_reference_boxes()
        if not boxes_by_page:
            return
        page_dimensions = self._get_page_dimensions()
        coords = []
        for page_num, boxes in sorted(boxes_by_page.items()):
            page_dim = page_dimensions.get(page_num, {'width': 595, 'height': 842})
            merged_boxes = self._cluster_and_merge_boxes(boxes, page_dim['width'], page_dim['height'])
            for box in merged_boxes:
                coords.append(
                    (page_num, box['x'], box['y'], box['x'] + box['w'], box['y'] + box['h'])
                )
        self.final_pymupdf_coords = coords

    def _draw_rects_and_save(self) -> tuple[bool, str]:
        try:
            doc = fitz.open(self.pdf_path)
            fill_color = (1, 1, 1) if DELETION_APPEARANCE == 'whiteout' else (0, 0, 0)
            for p, x0, y0, x1, y1 in self.final_pymupdf_coords:
                page_index = p - 1
                if page_index < len(doc):
                    page = doc[page_index]
                    rect = fitz.Rect(x0, y0, x1, y1) + (
                    -DELETION_PADDING, -DELETION_PADDING, DELETION_PADDING, DELETION_PADDING)
                    page.draw_rect(rect, color=fill_color, fill=fill_color, overlay=True)
            doc.save(self.output_pdf_path)
            doc.close()
            return True, f"Success ({len(self.final_pymupdf_coords)} area(s) covered)."
        except Exception as e:
            return False, f"An error occurred during PDF processing: {e}"

    def _extract_raw_reference_boxes(self) -> dict:
        try:
            tree = ET.parse(self.xml_path)
            root = tree.getroot()
            ns = {'tei': 'http://www.tei-c.org/ns/1.0'}
            ref_items = root.findall('.//tei:back//tei:biblStruct', ns)
            if not ref_items: ref_items = root.findall('.//tei:biblStruct', ns)
            if not ref_items: return {}
            boxes_by_page = defaultdict(list)
            for ref_item in ref_items:
                coords_str = ref_item.get('coords')
                if coords_str:
                    for box in coords_str.split(';'):
                        parts = box.split(',')
                        if len(parts) == 5:
                            p, x, y, w, h = int(parts[0]), float(parts[1]), float(parts[2]), float(parts[3]), float(
                                parts[4])
                            boxes_by_page[p].append({'x': x, 'y': y, 'w': w, 'h': h})
            return dict(boxes_by_page)
        except Exception:
            return {}

    def _get_page_dimensions(self) -> dict:
        try:
            tree = ET.parse(self.xml_path)
            root = tree.getroot()
            ns = {'tei': 'http://www.tei-c.org/ns/1.0'}
            page_dimensions = {}
            surface_nodes = root.findall('.//tei:surface', ns)
            for surface in surface_nodes:
                try:
                    page_num = int(surface.get('n'))
                    width = float(surface.get('lrx', 0))
                    height = float(surface.get('lry', 0))
                    if width > 0 and height > 0: page_dimensions[page_num] = {'width': width, 'height': height}
                except (ValueError, TypeError):
                    continue
            return page_dimensions
        except Exception:
            return {}

    def _clean_cluster(self, cluster_boxes: list, page_height: float) -> list:
        if len(cluster_boxes) <= 1: return cluster_boxes
        if len(cluster_boxes) == 2:
            cleaned_boxes = []
            for box in cluster_boxes:
                is_in_header = box['y'] < VERTICAL_Y_THRESHOLD_TOP
                is_in_footer = box['y'] > (page_height - VERTICAL_Y_THRESHOLD_BOTTOM)
                if not is_in_header and not is_in_footer: cleaned_boxes.append(box)
            return cleaned_boxes
        cluster_boxes.sort(key=lambda b: b['y'])
        gaps = [cluster_boxes[i]['y'] - cluster_boxes[i - 1]['y'] for i in range(1, len(cluster_boxes)) if
                cluster_boxes[i]['y'] > cluster_boxes[i - 1]['y']]
        if not gaps: return cluster_boxes
        standard_line_height = statistics.median(gaps)
        jump_threshold = standard_line_height * LINE_HEIGHT_MULTIPLIER
        sub_clusters = []
        current_sub_cluster = [cluster_boxes[0]]
        for i in range(1, len(cluster_boxes)):
            gap = cluster_boxes[i]['y'] - cluster_boxes[i - 1]['y']
            if gap > jump_threshold:
                sub_clusters.append(current_sub_cluster)
                current_sub_cluster = [cluster_boxes[i]]
            else:
                current_sub_cluster.append(cluster_boxes[i])
        sub_clusters.append(current_sub_cluster)
        if not sub_clusters: return []
        largest_cluster = max(sub_clusters, key=len)
        return largest_cluster

    def _cluster_and_merge_boxes(self, boxes: list, page_width: float, page_height: float) -> list:
        if not boxes: return []
        threshold = page_width * HORIZONTAL_THRESHOLD_RATIO
        clusters = []
        current_cluster = [boxes[0]]
        for i in range(1, len(boxes)):
            prev_box_left_edge = boxes[i - 1]['x']
            current_box_left_edge = boxes[i]['x']
            if current_box_left_edge - prev_box_left_edge > threshold:
                cleaned = self._clean_cluster(current_cluster, page_height)
                if cleaned: clusters.append(cleaned)
                current_cluster = [boxes[i]]
            else:
                current_cluster.append(boxes[i])
        cleaned = self._clean_cluster(current_cluster, page_height)
        if cleaned: clusters.append(cleaned)
        merged_boxes = []
        for i, cluster in enumerate(clusters):
            min_x = min(b['x'] for b in cluster)
            min_y = min(b['y'] for b in cluster)
            max_x1 = max(b['x'] + b['w'] for b in cluster)
            max_y1 = max(b['y'] + b['h'] for b in cluster)
            merged_box = {'cluster_id': i + 1, 'x': min_x, 'y': min_y, 'w': max_x1 - min_x, 'h': max_y1 - min_y}
            merged_boxes.append(merged_box)
        return merged_boxes

# ==============================================================================
#  MANAGER CLASS: Orchestrates the entire batch process
# ==============================================================================

class BatchRedactionManager:
    """
    Manages the end-to-end batch process of generating XML from PDFs via GROBID
    and then redacting the PDFs based on that XML, all concurrently.
    """
    # ... (paste the entire BatchRedactionManager class code here, unchanged) ...
    def __init__(self, pdf_dir: str, xml_dir: str, redacted_dir: str, grobid_url: str, workers: int):
        self.pdf_dir = pdf_dir
        self.xml_dir = xml_dir
        self.redacted_dir = redacted_dir
        self.grobid_url = grobid_url
        self.workers = workers
        print(f"Manager initialized with {self.workers} workers.")

    def run(self):
        total_start_time = time.time()
        self._setup_directories()
        pdf_files_to_process = [f for f in os.listdir(self.pdf_dir) if f.lower().endswith('.pdf')]
        if not pdf_files_to_process:
            print(f"No PDF files found in '{self.pdf_dir}'. Halting.")
            return
        self._generate_xmls_concurrently(pdf_files_to_process)
        xml_files_to_process = [f for f in os.listdir(self.xml_dir) if f.lower().endswith('.xml')]
        if not xml_files_to_process:
            print(f"No XML files found in '{self.xml_dir}' to process. Halting.")
            return
        self._redact_pdfs_concurrently(xml_files_to_process)
        total_time = time.time() - total_start_time
        print("\n" + "=" * 50)
        print("✅ All phases complete!")
        print(f"Total execution time: {total_time:.2f} seconds.")
        print("=" * 50)

    def _setup_directories(self):
        print("Setting up directories...")
        for dir_path in [self.pdf_dir, self.xml_dir, self.redacted_dir]:
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
                print(f"  - Created directory: '{dir_path}'")

    def _generate_xmls_concurrently(self, pdf_files: list):
        print("\n" + "-" * 20 + " Phase 1: Generating XMLs " + "-" * 20)
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            future_to_filename = {}
            for filename in pdf_files:
                pdf_path = os.path.join(self.pdf_dir, filename)
                xml_filename = os.path.splitext(filename)[0] + '.xml'
                output_xml_path = os.path.join(self.xml_dir, xml_filename)
                future = executor.submit(self._get_raw_xml_from_grobid, pdf_path, output_xml_path, self.grobid_url)
                future_to_filename[future] = filename
            for future in as_completed(future_to_filename):
                filename = future_to_filename[future]
                try:
                    success, message = future.result()
                    if success:
                        print(f"  [XML ✅] {filename}")
                    else:
                        print(f"  [XML ❌] {filename}: {message}")
                except Exception as exc:
                    print(f"  [XML ❌] {filename}: An exception occurred in the thread: {exc}")
        phase_time = time.time() - start_time
        print(f"--- Phase 1 complete in {phase_time:.2f} seconds ---")

    @staticmethod
    def _get_raw_xml_from_grobid(pdf_path: str, output_xml_path: str, grobid_url: str) -> tuple[bool, str]:
        try:
            with open(pdf_path, 'rb') as f:
                files = {'input': (os.path.basename(pdf_path), f, 'application/pdf')}
                payload = {'teiCoordinates': 'biblStruct'}
                response = requests.post(grobid_url, files=files, data=payload, timeout=180)
            if response.status_code == 200:
                with open(output_xml_path, 'wb') as xml_file:
                    xml_file.write(response.content)
                return True, "Success"
            else:
                return False, f"GROBID returned status {response.status_code}"
        except requests.exceptions.RequestException as e:
            return False, f"Request Error: {e}"
        except Exception as e:
            return False, f"An unexpected error occurred: {e}"

    def _redact_pdfs_concurrently(self, xml_files: list):
        print("\n" + "-" * 20 + " Phase 2: Redacting PDFs " + "-" * 20)
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            future_to_filename = {
                executor.submit(self._process_single_redaction, xml_filename): xml_filename
                for xml_filename in xml_files
            }
            for future in as_completed(future_to_filename):
                filename = future_to_filename[future]
                try:
                    success, message = future.result()
                    if success:
                        print(f"  [Redact ✅] {filename}: {message}")
                    else:
                        print(f"  [Redact ❌] {filename}: {message}")
                except Exception as exc:
                    print(
                        f"  [Redact ❌] {filename.replace}: An exception occurred in the thread: {exc}")
        phase_time = time.time() - start_time
        print(f"--- Phase 2 complete in {phase_time:.2f} seconds ---")

    def _process_single_redaction(self, xml_filename: str) -> tuple[bool, str]:
        xml_path = os.path.join(self.xml_dir, xml_filename)
        pdf_filename = os.path.splitext(xml_filename)[0] + '.pdf'
        pdf_path = os.path.join(self.pdf_dir, pdf_filename)
        output_pdf_path = os.path.join(self.redacted_dir, pdf_filename.replace('.pdf', '_cleaned.pdf'))
        redactor = ReferenceRedactor(xml_path, pdf_path, output_pdf_path)
        return redactor.process()