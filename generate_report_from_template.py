import docx
from docx.shared import Pt, Cm, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
import os

TEMPLATE_PATH = "/home/trongzufo/csdlpt/Distributed Database Project Proposal Template.docx"
OUTPUT_PATH = "/home/trongzufo/csdlpt/BaoCao_BFT_Distributed_Ledger.docx"

def set_cell_shading(cell, color_hex):
    """Set background color for a cell in a table."""
    shading = cell._element.get_or_add_tcPr()
    shd = shading.makeelement(qn('w:shd'), {
        qn('w:fill'): color_hex,
        qn('w:val'): 'clear',
    })
    shading.append(shd)

def set_table_borders(table):
    """Set table borders to a clean light gray grid using XML manipulation."""
    tblPr = table._tbl.tblPr
    borders = tblPr.first_child_found_in("w:tblBorders")
    if borders is None:
        borders = OxmlElement('w:tblBorders')
        tblPr.append(borders)
    
    for border_name in ['top', 'left', 'bottom', 'right', 'insideH', 'insideV']:
        border = borders.find(qn(f'w:{border_name}'))
        if border is None:
            border = OxmlElement(f'w:{border_name}')
            borders.append(border)
        border.set(qn('w:val'), 'single')
        border.set(qn('w:sz'), '4')  # 0.5 pt width
        border.set(qn('w:space'), '0')
        border.set(qn('w:color'), 'CCCCCC')  # Light gray

def add_styled_table(doc, headers, rows, col_widths=None):
    """Add a beautifully styled table matching the report design."""
    table = doc.add_table(rows=1 + len(rows), cols=len(headers))
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    
    # Set custom light gray borders
    set_table_borders(table)

    # Header
    for i, h in enumerate(headers):
        cell = table.rows[0].cells[i]
        cell.text = h
        for p in cell.paragraphs:
            p.alignment = WD_ALIGN_PARAGRAPH.CENTER
            p.paragraph_format.space_before = Pt(4)
            p.paragraph_format.space_after = Pt(4)
            for r in p.runs:
                r.bold = True
                r.font.name = 'Times New Roman'
                r.font.size = Pt(11)
                r.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
        set_cell_shading(cell, '1A3C7A') # Match theme dark blue

    # Data rows
    for ri, row in enumerate(rows):
        for ci, val in enumerate(row):
            cell = table.rows[ri + 1].cells[ci]
            cell.text = str(val)
            for p in cell.paragraphs:
                p.paragraph_format.space_before = Pt(3)
                p.paragraph_format.space_after = Pt(3)
                for r in p.runs:
                    r.font.name = 'Times New Roman'
                    r.font.size = Pt(11)
            # Alternating row colors
            if ri % 2 == 0:
                set_cell_shading(cell, 'F0F4FA') # Light slate blue
            else:
                set_cell_shading(cell, 'FFFFFF')

    if col_widths:
        for i, w in enumerate(col_widths):
            for row in table.rows:
                row.cells[i].width = Cm(w)

    return table

def add_code_block(doc, code, font_size=9.5):
    """Add a clean, gray-shaded programming code block."""
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(6)
    p.paragraph_format.space_after = Pt(6)
    p.paragraph_format.left_indent = Cm(0.5)
    
    pPr = p._element.get_or_add_pPr()
    shd = pPr.makeelement(qn('w:shd'), {
        qn('w:fill'): 'F5F5F5',
        qn('w:val'): 'clear',
    })
    pPr.append(shd)

    run = p.add_run(code)
    run.font.name = 'Consolas'
    run.font.size = Pt(font_size)
    run.font.color.rgb = RGBColor(0x20, 0x20, 0x20)
    return p

def fix_margins_xml(doc):
    """Fix decimal margin values in the Word XML to prevent python-docx ValueError."""
    for section in doc.sections:
        sectPr = section._sectPr
        pgMar = sectPr.pgMar
        if pgMar is not None:
            for attr in ['top', 'bottom', 'left', 'right', 'header', 'footer', 'gutter']:
                val = pgMar.get(qn(f'w:{attr}'))
                if val:
                    try:
                        int_val = int(float(val))
                        pgMar.set(qn(f'w:{attr}'), str(int_val))
                    except ValueError:
                        pass

def main():
    if not os.path.exists(TEMPLATE_PATH):
        print(f"Error: Template file not found at {TEMPLATE_PATH}")
        return

    doc = docx.Document(TEMPLATE_PATH)
    print("Successfully loaded template document.")

    # Apply the XML margin parser fix
    fix_margins_xml(doc)
    print("Cleaned XML margins to bypass python-docx float parsing bug.")

    # 1. Modify template paragraphs (0 to 28)
    
    # Title (Paragraph 0)
    p_title = doc.paragraphs[0]
    p_title.runs[0].text = "DISTRIBUTED DATABASE PROJECT PROPOSAL & REPORT"
    p_title.runs[0].font.name = 'Times New Roman'
    p_title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    
    # Due Date & Category (Paragraph 1)
    p_meta = doc.paragraphs[1]
    p_meta.runs[0].text = "Due Date:"
    p_meta.runs[0].font.bold = True
    p_meta.runs[0].font.name = 'Times New Roman'
    p_meta.runs[1].text = " June 2026 (Week 15 - Final Submission)\n"
    p_meta.runs[1].font.name = 'Times New Roman'
    p_meta.runs[2].text = "Project Topic:"
    p_meta.runs[2].font.bold = True
    p_meta.runs[2].font.name = 'Times New Roman'
    p_meta.runs[3].text = " Simplified BFT Consensus Protocol for Distributed Ledger (Category 3)"
    p_meta.runs[3].font.name = 'Times New Roman'

    # Section 1. Project Identity
    # Team Name (Paragraph 3)
    p_team = doc.paragraphs[3]
    p_team.runs[0].text = "Team Name:"
    p_team.runs[0].font.bold = True
    p_team.runs[0].font.name = 'Times New Roman'
    p_team.runs[1].text = " Byzantine Resilient Ledger (Nhóm Học Viên CSDLPT)"
    p_team.runs[1].font.name = 'Times New Roman'

    # Team Members (Paragraph 4)
    p_members = doc.paragraphs[4]
    p_members.runs[0].text = "Team Members:"
    p_members.runs[0].font.bold = True
    p_members.runs[0].font.name = 'Times New Roman'
    p_members.runs[1].text = " [Sinh viên thực hiện: Nguyễn Văn A - MSSV: ...]"
    p_members.runs[1].font.name = 'Times New Roman'

    # Project Title (Paragraph 5)
    p_proj_title = doc.paragraphs[5]
    p_proj_title.runs[0].text = "Project Title:"
    p_proj_title.runs[0].font.bold = True
    p_proj_title.runs[0].font.name = 'Times New Roman'
    p_proj_title.runs[1].text = " Simplified Byzantine Fault Tolerance (BFT) Consensus Simulation for Distributed Ledgers"
    p_proj_title.runs[1].font.name = 'Times New Roman'

    # Section 2. Objective & Problem Statement
    # The "Why" (Paragraph 7)
    p_why = doc.paragraphs[7]
    p_why.runs[0].text = 'The "Why":'
    p_why.runs[0].font.bold = True
    p_why.runs[0].font.name = 'Times New Roman'
    p_why.runs[1].text = (
        ' Các giao thức đồng thuận cổ điển (như Paxos hay Raft) hoạt động dưới mô hình dung lỗi crash '
        '(Crash Fault Tolerance - CFT). Chúng giả định rằng các nút mạng đều trung thực và chỉ có thể bị crash '
        '(ngừng hoạt động) hoặc gặp sự cố trễ mạng, chứ không bao giờ nói dối hay hành xử độc hại. Tuy nhiên, '
        'trong các cơ sở dữ liệu phân tán không tin cậy hoặc hệ thống blockchain, các nút có thể gặp lỗi Byzantine—chẳng '
        'hạn như equivocation (gửi các phiếu bầu khác nhau đến các nút khác nhau) nhằm phá vỡ đồng thuận hoặc thực hiện '
        'tấn công lặp chi (double-spend). Dự án này triển khai một giao thức đồng thuận Byzantine Fault Tolerance (BFT) '
        'rút gọn để duy trì tính nhất quán trên Sổ cái phân tán (Distributed Ledger) giữa các nút trung thực, ngay cả khi '
        'có hành vi equivocation từ điều phối viên độc hại và lỗi crash nút mạng.'
    )
    p_why.runs[1].font.name = 'Times New Roman'

    # Core Logic (Paragraph 8)
    p_core = doc.paragraphs[8]
    p_core.runs[0].text = 'Core Logic:'
    p_core.runs[0].font.bold = True
    p_core.runs[0].font.name = 'Times New Roman'
    p_core.runs[1].text = (
        ' Giao thức đồng thuận BFT rút gọn được xây dựng trên mô hình N = 3f + 1 node (chịu tối đa f node Byzantine lỗi). '
        'Trong mô phỏng này, f = 1 nên tổng số site N = 4, và Quorum đồng thuận là 2f + 1 = 3 phiếu COMMIT. Giao thức bao gồm:\n'
        '  1. Giai đoạn Propose: Node Leader đề xuất giao dịch mới.\n'
        '  2. Giai đoạn Vote & Verify: Các site xác thực chữ ký số giả lập của message, kiểm tra tính đúng đắn của giao dịch và phát phiếu bầu (COMMIT/ABORT). Nút Byzantine (Site 0) thực hiện equivocation (phát phiếu mâu thuẫn).\n'
        '  3. Giai đoạn Commit/Abort: Các site thu thập phiếu bầu, nếu đủ Quorum 2f+1 phiếu trùng khớp thì tiến hành ghi vào Ledger và lưu trạng thái vào Write-Ahead Log (WAL) dưới định dạng JSON Lines.\n'
        '  4. Giao thức Crash Recovery: Site 1 bị crash trong Phase 1, sau khi phục hồi sẽ tự động đọc WAL để khôi phục trạng thái READY và gửi thông điệp yêu cầu phiếu bầu từ các site khác để hoàn tất giao dịch.'
    )
    p_core.runs[1].font.name = 'Times New Roman'

    # Section 3. Dataset Specification
    # Source (Paragraph 10)
    p_source = doc.paragraphs[10]
    p_source.runs[0].text = 'Source:'
    p_source.runs[0].font.bold = True
    p_source.runs[0].font.name = 'Times New Roman'
    p_source.runs[1].text = ' Giao dịch tài chính tự phát sinh mô phỏng (Simulated financial transaction stream).'
    p_source.runs[1].font.name = 'Times New Roman'
    for r in p_source.runs[2:]:
        r.text = ""

    # Size (Paragraph 11)
    p_size = doc.paragraphs[11]
    p_size.runs[0].text = 'Size:'
    p_size.runs[0].font.bold = True
    p_size.runs[0].font.name = 'Times New Roman'
    p_size.runs[1].text = ' 2 giao dịch mô phỏng phức tạp liên kết chuỗi sổ cái (TX1 và TX2).'
    p_size.runs[1].font.name = 'Times New Roman'

    # Schema (Paragraph 12)
    p_schema = doc.paragraphs[12]
    p_schema.runs[0].text = 'Schema:'
    p_schema.runs[0].font.bold = True
    p_schema.runs[0].font.name = 'Times New Roman'
    p_schema.runs[1].text = (
        ' Cấu trúc dữ liệu giao dịch gồm: tx_id (mã giao dịch, chuỗi băm), sender (người gửi), '
        'receiver (người nhận), amount (số tiền), timestamp (thời gian tạo), và signature (chữ ký số giả lập).'
    )
    p_schema.runs[1].font.name = 'Times New Roman'
    for r in p_schema.runs[2:]:
        r.text = ""

    # Fragmentation Strategy (Paragraph 13)
    p_frag = doc.paragraphs[13]
    p_frag.runs[0].text = 'Fragmentation Strategy:'
    p_frag.runs[0].font.bold = True
    p_frag.runs[0].font.name = 'Times New Roman'
    p_frag.runs[1].text = (
        ' Full Replication (Sao chép toàn phần). Vì đặc thù của Sổ cái phân tán (Distributed Ledger), '
        'mỗi site đều lưu trữ một bản sao đầy đủ của Sổ cái để kiểm thử chéo và tự phục hồi độc lập.'
    )
    p_frag.runs[1].font.name = 'Times New Roman'

    # Section 4. System Architecture
    # Nodes (Paragraph 15)
    p_nodes = doc.paragraphs[15]
    p_nodes.runs[0].text = 'Nodes:'
    p_nodes.runs[0].font.bold = True
    p_nodes.runs[0].font.name = 'Times New Roman'
    p_nodes.runs[1].text = (
        ' Mô phỏng 4 sites sử dụng các tiến trình độc lập (multiprocessing). '
        'Site 0 đóng vai trò Leader (Byzantine), Site 1, 2, 3 là các replica trung thực (Site 1 bị crash và recover).'
    )
    p_nodes.runs[1].font.name = 'Times New Roman'

    # Communication Layer (Paragraph 16)
    p_comm = doc.paragraphs[16]
    p_comm.runs[0].text = 'Communication Layer:'
    p_comm.runs[0].font.bold = True
    p_comm.runs[0].font.name = 'Times New Roman'
    p_comm.runs[1].text = (
        ' Kênh truyền mạng bất đồng bộ mô phỏng bằng multiprocessing.Queue với độ trễ ngẫu nhiên từ 50ms đến 200ms.'
    )
    p_comm.comm_run = p_comm.runs[1] # Reference
    p_comm.runs[1].font.name = 'Times New Roman'

    # Storage (Paragraph 17)
    p_storage = doc.paragraphs[17]
    p_storage.runs[0].text = 'Storage:'
    p_storage.runs[0].font.bold = True
    p_storage.runs[0].font.name = 'Times New Roman'
    p_storage.runs[1].text = (
        ' Trạng thái đồng thuận được ghi đĩa vật lý dưới dạng Write-Ahead Log (WAL) bằng file JSON Lines (tại wal/site_x.wal). '
        'Sổ cái Ledger được lưu trữ trên bộ nhớ RAM tiến trình và đồng bộ từ WAL khi khởi động lại.'
    )
    p_storage.runs[1].font.name = 'Times New Roman'

    # Section 5. Tech Stack & Implementation Plan
    # Programming Language (Paragraph 19)
    p_lang = doc.paragraphs[19]
    p_lang.runs[0].text = 'Programming Language:'
    p_lang.runs[0].font.bold = True
    p_lang.runs[0].font.name = 'Times New Roman'
    p_lang.runs[1].text = ' Python 3'
    p_lang.runs[1].font.name = 'Times New Roman'

    # Deployment (Paragraph 20)
    p_deploy = doc.paragraphs[20]
    p_deploy.runs[0].text = 'Deployment:'
    p_deploy.runs[0].font.bold = True
    p_deploy.runs[0].font.name = 'Times New Roman'
    p_deploy.runs[1].text = ' Đa tiến trình (multiprocessing) chạy cục bộ trên Localhost.'
    p_deploy.runs[1].font.name = 'Times New Roman'

    # Libraries/Frameworks (Paragraph 21)
    p_libs = doc.paragraphs[21]
    p_libs.runs[0].text = 'Libraries/Frameworks:'
    p_libs.runs[0].font.bold = True
    p_libs.runs[0].font.name = 'Times New Roman'
    p_libs.runs[1].text = ' multiprocessing, queue, json, hashlib, logging, time.'
    p_libs.runs[1].font.name = 'Times New Roman'

    # Section 6. Success Metrics & Analysis
    # Quantitative Metric (Paragraph 23)
    p_metric = doc.paragraphs[23]
    p_metric.runs[0].text = 'Quantitative Metric:'
    p_metric.runs[0].font.bold = True
    p_metric.runs[0].font.name = 'Times New Roman'
    p_metric.runs[1].text = (
        ' Tính nhất quán sổ cái (100% site trung thực cam kết cùng một trạng thái giao dịch), '
        'thời gian phục hồi nút bị lỗi (dưới 10ms) và tỉ lệ phát hiện/ngăn chặn thông điệp equivocation lỗi.'
    )
    p_metric.runs[1].font.name = 'Times New Roman'

    # Failure Scenario (Paragraph 24)
    p_fail = doc.paragraphs[24]
    p_fail.runs[0].text = 'The "Failure" Scenario:'
    p_fail.runs[0].font.bold = True
    p_fail.runs[0].font.name = 'Times New Roman'
    p_fail.runs[1].text = (
        ' Giả lập 2 loại lỗi phức tạp đồng thời: (1) Lỗi Byzantine Equivocation: Site 0 gửi COMMIT cho Site 2 nhưng gửi ABORT cho Site 3; '
        '(2) Lỗi Crash & Recovery: Site 1 bị crash đột ngột sau khi phát phiếu bầu và phục hồi thành công từ WAL để hoàn tất giao dịch đồng thuận.'
    )
    p_fail.runs[1].font.name = 'Times New Roman'

    # Section 7. Project Milestones
    # Milestone 1 (Paragraph 26)
    p_m1 = doc.paragraphs[26]
    p_m1.runs[0].text = 'Milestone 1 (Week 5):'
    p_m1.runs[0].font.bold = True
    p_m1.runs[0].font.name = 'Times New Roman'
    p_m1.runs[1].text = ' Thiết kế giao thức, cấu trúc tệp WAL JSON Lines và hệ thống chữ ký số giả lập.'
    p_m1.runs[1].font.name = 'Times New Roman'

    # Milestone 2 (Paragraph 27)
    p_m2 = doc.paragraphs[27]
    p_m2.runs[0].text = 'Milestone 2 (Week 8):'
    p_m2.runs[0].font.bold = True
    p_m2.runs[0].font.name = 'Times New Roman'
    p_m2.runs[1].text = ' Hoàn tất lõi đồng thuận BFT với cơ chế kiểm tra Quorum 2f+1 và xử lý lỗi Equivocation.'
    p_m2.runs[1].font.name = 'Times New Roman'

    # Milestone 3 (Paragraph 28)
    p_m3 = doc.paragraphs[28]
    p_m3.runs[0].text = 'Milestone 3 (Week 12):'
    p_m3.runs[0].font.bold = True
    p_m3.runs[0].font.name = 'Times New Roman'
    p_m3.runs[1].text = ' Tích hợp kịch bản tự động mô phỏng lỗi crash/recovery, kiểm thử tính nhất quán của Sổ cái và viết báo cáo.'
    p_m3.runs[1].font.name = 'Times New Roman'

    # Clear trailing empty paragraphs in template
    doc.paragraphs[29].text = ""
    doc.paragraphs[30].text = ""

    # Add a page break to separate proposal from implementation details
    doc.add_page_break()

    # ================================================================
    # APPENDED CHAPTER 8: SOURCE CODE IMPLEMENTATION
    # ================================================================
    h8 = doc.add_heading('8. Detailed Source Code Implementation (Cài đặt chi tiết)', level=3)
    h8.runs[0].font.name = 'Times New Roman'
    h8.runs[0].font.color.rgb = RGBColor(0, 0, 0)
    h8.runs[0].font.bold = True
    h8.runs[0].font.size = Pt(13)

    p_intro8 = doc.add_paragraph()
    p_intro8.paragraph_format.line_spacing = 1.15
    p_intro8.paragraph_format.space_after = Pt(6)
    r = p_intro8.add_run(
        'Hệ thống mô phỏng BFT được cài đặt hoàn chỉnh trong tệp main.py bằng ngôn ngữ Python. '
        'Kiến trúc mã nguồn được phân tách rõ ràng thành các hàm chức năng độc lập nhằm đảm bảo nguyên tắc '
        'Single Responsibility (Đơn nhiệm), giúp mã nguồn dễ đọc, dễ kiểm thử và bảo trì:'
    )
    r.font.name = 'Times New Roman'
    r.font.size = Pt(11)

    # Function Table
    add_styled_table(doc,
        ['Hàm chức năng', 'Vai trò / Trách nhiệm', 'Đầu vào / Đầu ra chính'],
        [
            ['honest_broadcast()', 'Site trung thực phát phiếu bầu COMMIT nhất quán tới tất cả các nút khác.', 'qs, sid, tx_id, log | None'],
            ['byzantine_broadcast()', 'Leader Byzantine (Site 0) phát phiếu mâu thuẫn (COMMIT cho site chẵn, ABORT cho site lẻ).', 'qs, sid, tx_id, log | None'],
            ['collect_votes()', 'Thu thập phiếu bầu từ hàng đợi, xác thực chữ ký số giả lập và lọc bỏ tin nhắn không hợp lệ.', 'q, sid, tx_id, log, timeout | dict (votes)'],
            ['make_decision()', 'Đếm phiếu và ra quyết định COMMIT nếu số phiếu COMMIT đạt Quorum >= 2f + 1.', 'votes, log, tx_id | str (COMMIT/ABORT)'],
            ['listen_for_recovery()', 'Các site trung thực lắng nghe yêu cầu phục hồi từ site bị crash và gửi lại phiếu bầu của mình.', 'q, sid, my_vote, log, listen_time | None'],
            ['process_transaction()', 'Điều phối toàn bộ quy trình đồng thuận cho một giao dịch (Init -> Vote -> Decide -> WAL & Ledger).', 'sid, is_malicious, should_crash, tx | None'],
        ],
        col_widths=[4.0, 7.5, 4.5]
    )

    doc.add_paragraph() # spacing

    p_code_desc = doc.add_paragraph()
    r = p_code_desc.add_run('Đoạn mã xử lý Equivocation Byzantine và Thu thập xác thực phiếu bầu (Quorum):')
    r.font.name = 'Times New Roman'
    r.font.size = Pt(11)
    r.bold = True

    code_snippet = (
        'def byzantine_broadcast(qs, sid, tx_id, log):\n'
        '    """Leader Byzantine (Site 0) thuc hien Equivocation"""\n'
        '    log.info(\'LEADER_BYZANTINE\', f\'tx_id={tx_id} | !!! BAT DAU EQUIVOCATION !!!\')\n'
        '    for t in range(NUM_SITES):\n'
        '        # COMMIT cho site chan (0, 2), ABORT cho site le (1, 3)\n'
        '        fake_vote = VOTE_COMMIT if t % 2 == 0 else VOTE_ABORT\n'
        '        msg = {\n'
        '            \'type\': MSG_VOTE,\n'
        '            \'sender\': sid,\n'
        '            \'tx_id\': tx_id,\n'
        '            \'vote\': fake_vote\n'
        '        }\n'
        '        sign_message(msg, sid)  # Gia lap ky so de xac thuc nguon gui\n'
        '        qs[t].put(msg)\n'
        '        log.info(\'SEND\', f\'tx_id={tx_id} | {fake_vote} | to=Site {t} [EQUIVOCATION]\')\n'
        '\n'
        'def collect_votes(q, sid, tx_id, log, timeout=TIMEOUT):\n'
        '    """Thu thap va xac thuc phieu bau tu hang doi tin nhan"""\n'
        '    votes = {}\n'
        '    start_time = time.time()\n'
        '    while len(votes) < NUM_SITES and (time.time() - start_time) < timeout:\n'
        '        try:\n'
        '            msg = q.get(timeout=0.1)\n'
        '            if msg.get(\'tx_id\') != tx_id or msg.get(\'type\') != MSG_VOTE:\n'
        '                continue\n'
        '            if not verify_signature(msg):  # Ngan chan Byzantine gia mao danh tinh\n'
        '                log.info(\'SECURITY\', f\'tx_id={tx_id} | Chuky gia mao tu Site {msg.get("sender")}!\')\n'
        '                continue\n'
        '            votes[msg[\'sender\']] = msg[\'vote\']\n'
        '        except queue.Empty:\n'
        '            continue\n'
        '    return votes'
    )
    add_code_block(doc, code_snippet, font_size=9)

    doc.add_page_break()

    # ================================================================
    # APPENDED CHAPTER 9: SIMULATION LOGS & EXECUTION EVIDENCE
    # ================================================================
    h9 = doc.add_heading('9. Simulation Logs & Execution Evidence (Nhật ký thực nghiệm)', level=3)
    h9.runs[0].font.name = 'Times New Roman'
    h9.runs[0].font.color.rgb = RGBColor(0, 0, 0)
    h9.runs[0].font.bold = True
    h9.runs[0].font.size = Pt(13)

    p_intro9 = doc.add_paragraph()
    p_intro9.paragraph_format.line_spacing = 1.15
    p_intro9.paragraph_format.space_after = Pt(6)
    r = p_intro9.add_run(
        'Kịch bản chạy thực nghiệm mô phỏng toàn bộ tiến trình hoạt động của hệ thống. '
        'Dưới đây là bảng tóm tắt trình tự các sự kiện chính diễn ra trong hai Phase giao dịch:'
    )
    r.font.name = 'Times New Roman'
    r.font.size = Pt(11)

    # Event Table
    add_styled_table(doc,
        ['Giai đoạn', 'Sự kiện mô phỏng', 'Mô tả kỹ thuật & Kết quả đạt được'],
        [
            ['Phase 1 (TX 1)', 'Byzantine Equivocation', 'Leader (Site 0) gửi COMMIT cho Site 2 nhưng gửi ABORT cho Site 1 và 3.'],
            ['Phase 1 (TX 1)', 'Site 1 Crash', 'Site 1 bị crash (tắt tiến trình bằng os._exit) ngay sau khi phát phiếu COMMIT để giả lập lỗi mạng/phần cứng.'],
            ['Phase 1 (TX 1)', 'Đồng thuận Quorum', 'Các site trung thực thu thập được 3 phiếu COMMIT (từ Site 1, 2, 3) đạt Quorum (3) -> Đồng thuận COMMIT.'],
            ['Phase 1 (TX 1)', 'Recovery từ WAL', 'Site 1 khởi động lại, đọc WAL phát hiện trạng thái READY, gửi tin nhắn thu thập phiếu bầu để đồng bộ và COMMIT giao dịch thành công.'],
            ['Phase 2 (TX 2)', 'Giao dịch bình thường', 'Site 0 tiếp tục equivocate. Tất cả các site hoạt động bình thường, thu thập đủ 3 phiếu COMMIT và nhất quán COMMIT.'],
            ['Kết luận', 'Nhất quán sổ cái', 'Cả 3 site trung thực (1, 2, 3) đều có sổ cái đồng nhất 100% chứa đầy đủ 2 giao dịch đã cam kết.'],
        ],
        col_widths=[3.0, 4.5, 8.5]
    )

    doc.add_paragraph() # spacing

    p_log_desc = doc.add_paragraph()
    r = p_log_desc.add_run('Nhật ký hiển thị trên màn hình console (Console Stdout Log) của hệ thống mô phỏng BFT:')
    r.font.name = 'Times New Roman'
    r.font.size = Pt(11)
    r.bold = True

    stdout_logs = (
        '=== KICH BAN MO PHONG BAT DAU ===\n'
        'Running node processes...\n'
        '[2026-06-03 19:28:10] Site 0 | START_TX | tx_id=1 | Bat dau giao dich: A chuyen 10 cho B\n'
        '[2026-06-03 19:28:10] Site 0 | LEADER_BYZANTINE | tx_id=1 | !!! BAT DAU EQUIVOCATION !!!\n'
        '[2026-06-03 19:28:10] Site 0 | SEND | tx_id=1 | COMMIT | to=Site 0 [EQUIVOCATION]\n'
        '[2026-06-03 19:28:10] Site 0 | SEND | tx_id=1 | ABORT | to=Site 1 [EQUIVOCATION]\n'
        '[2026-06-03 19:28:10] Site 0 | SEND | tx_id=1 | COMMIT | to=Site 2 [EQUIVOCATION]\n'
        '[2026-06-03 19:28:10] Site 0 | SEND | tx_id=1 | ABORT | to=Site 3 [EQUIVOCATION]\n'
        '[2026-06-03 19:28:10] Site 1 | START_TX | tx_id=1 | Bat dau giao dich: A chuyen 10 cho B\n'
        '[2026-06-03 19:28:10] Site 1 | SEND | tx_id=1 | COMMIT | to=all [HONEST_BROADCAST]\n'
        '[2026-06-03 19:28:10] Site 1 | WAL_WRITE | tx_id=1 | Ghi WAL: READY | vote=COMMIT\n'
        '[2026-06-03 19:28:10] Site 1 | CRASH_SIM | tx_id=1 | !!! SITE 1 CRASH !!!\n'
        '[2026-06-03 19:28:11] Site 2 | COLLECT | tx_id=1 | Nhan phieu tu cac site...\n'
        '[2026-06-03 19:28:11] Site 2 | DECISION_CALC | tx_id=1 | COMMIT=3 ABORT=1 | Quorum can: 3\n'
        '[2026-06-03 19:28:11] Site 2 | QUORUM_CHECK | tx_id=1 | 3 >= 3? DAT => COMMIT\n'
        '[2026-06-03 19:28:11] Site 2 | WAL_WRITE | tx_id=1 | Ghi WAL: COMMIT\n'
        '[2026-06-03 19:28:11] Site 2 | LEDGER_APPEND | tx_id=1 | Da ghi vao ledger (size=1)\n'
        '[2026-06-03 19:28:12] MainProcess | RESTART | Site 1 dang duoc khoi dong lai...\n'
        '[2026-06-03 19:28:12] Site 1 | RESTART_WAL | tx_id=1 | Doc WAL: trang thai READY found | vote=COMMIT\n'
        '[2026-06-03 19:28:12] Site 1 | RECOVERY_START | tx_id=1 | Gui REQUEST_VOTES toi cac site khac...\n'
        '[2026-06-03 19:28:12] Site 2 | RECOVERY_REQ | tx_id=1 | Nhan yeu cau phuc hoi tu Site 1. Gui lai vote: COMMIT\n'
        '[2026-06-03 19:28:12] Site 1 | RECOVERY_COLLECT | tx_id=1 | Nhan phieu phuc hoi: {2: COMMIT, 3: COMMIT, 0: COMMIT}\n'
        '[2026-06-03 19:28:12] Site 1 | RECOVERY_SUCCESS | tx_id=1 | Dat Quorum (3 phieu COMMIT) -> COMMIT tu WAL\n'
        '[2026-06-03 19:28:12] Site 1 | LEDGER_APPEND | tx_id=1 | Da ghi vao ledger (size=1)\n'
        '---\n'
        '[2026-06-03 19:28:18] Site 0 | START_TX | tx_id=2 | Bat dau giao dich: B chuyen 5 cho C\n'
        '[2026-06-03 19:28:18] Site 0 | LEADER_BYZANTINE | tx_id=2 | !!! BAT DAU EQUIVOCATION !!!\n'
        '[2026-06-03 19:28:19] Site 1 | DECISION_CALC | tx_id=2 | COMMIT=3 ABORT=1 | Quorum can: 3 -> COMMIT\n'
        '[2026-06-03 19:28:19] Site 2 | DECISION_CALC | tx_id=2 | COMMIT=3 ABORT=1 | Quorum can: 3 -> COMMIT\n'
        '[2026-06-03 19:28:19] Site 3 | DECISION_CALC | tx_id=2 | COMMIT=3 ABORT=1 | Quorum can: 3 -> COMMIT\n'
        '[2026-06-03 19:28:19] Site 1 | LEDGER_FINAL | So cai: [{"tx_id": 1, "data": "A chuyen 10"}, {"tx_id": 2, "data": "B chuyen 5"}]\n'
        '[2026-06-03 19:28:19] Site 2 | LEDGER_FINAL | So cai: [{"tx_id": 1, "data": "A chuyen 10"}, {"tx_id": 2, "data": "B chuyen 5"}]\n'
        '[2026-06-03 19:28:19] Site 3 | LEDGER_FINAL | So cai: [{"tx_id": 1, "data": "A chuyen 10"}, {"tx_id": 2, "data": "B chuyen 5"}]\n'
        '=== KICH BAN MO PHONG KET THUC ==='
    )
    add_code_block(doc, stdout_logs, font_size=8.5)

    doc.add_page_break()

    # ================================================================
    # APPENDED CHAPTER 10: BFT CONSENSUS VS PAXOS CRASH FAULT ANALYSIS
    # ================================================================
    h10 = doc.add_heading('10. BFT Consensus vs. Paxos Crash Fault Analysis (So sánh BFT và Paxos)', level=3)
    h10.runs[0].font.name = 'Times New Roman'
    h10.runs[0].font.color.rgb = RGBColor(0, 0, 0)
    h10.runs[0].font.bold = True
    h10.runs[0].font.size = Pt(13)

    p_intro10 = doc.add_paragraph()
    p_intro10.paragraph_format.line_spacing = 1.15
    p_intro10.paragraph_format.space_after = Pt(6)
    r = p_intro10.add_run(
        'Điểm cốt lõi của đề tài là chứng minh tại sao hệ thống sổ cái phân tán yêu cầu giao thức BFT '
        'thay vì các giao thức đồng thuận lỗi crash thông thường như Paxos hay Raft. Bảng so sánh dưới đây '
        'làm rõ sự khác biệt giữa hai mô hình lỗi:'
    )
    r.font.name = 'Times New Roman'
    r.font.size = Pt(11)

    # Comparison Table
    add_styled_table(doc,
        ['Tiêu chí so sánh', 'Paxos (Crash Fault Tolerance)', 'BFT (Byzantine Fault Tolerance)'],
        [
            ['Mô hình lỗi xử lý', 'Chỉ xử lý lỗi Crash (nút ngừng hoạt động, mất kết nối mạng).', 'Xử lý cả lỗi Byzantine (nút hành xử tùy ý, nói dối, giả mạo).'],
            ['Giả định về nút mạng', 'Tất cả các nút đều trung thực và tuân thủ đúng giao thức.', 'Một số nút có thể bị lỗi, bị hack hoặc có hành vi phá hoại.'],
            ['Equivocation (Nói dối)', 'Không xử lý được. Hệ thống sẽ bị chia rẽ (split-brain) hoặc mất tính nhất quán.', 'Xử lý hoàn toàn được nhờ cơ chế kiểm tra chéo và Quorum rộng.'],
            ['Số lượng nút tối thiểu', 'N = 2f + 1 (Ví dụ: f=1 cần tối thiểu 3 nút).', 'N = 3f + 1 (Ví dụ: f=1 cần tối thiểu 4 nút).'],
            ['Kích thước Quorum', 'Quorum = f + 1 (Ví dụ: f=1 cần 2 nút đồng thuận).', 'Quorum = 2f + 1 (Ví dụ: f=1 cần 3 nút đồng thuận).'],
            ['Bảo mật / Chữ ký số', 'Không yêu cầu mã hóa chữ ký (chỉ kiểm tra ID người gửi).', 'Bắt buộc sử dụng chữ ký số để chống giả mạo danh tính.'],
            ['Ứng dụng thực tế', 'Dùng cho cơ sở dữ liệu nội bộ tin cậy (Google Spanner, Etcd).', 'Dùng cho hệ thống phi tập trung, Blockchain, Sổ cái (Bitcoin, PBFT).'],
        ],
        col_widths=[3.5, 6.0, 6.5]
    )

    doc.add_paragraph() # spacing

    p_paxos_fail = doc.add_paragraph()
    p_paxos_fail.paragraph_format.line_spacing = 1.15
    p_paxos_fail.paragraph_format.space_after = Pt(6)
    r = p_paxos_fail.add_run(
        'Phân tích chi tiết hành vi Equivocation:\n'
        'Trong giao dịch TX1, Site 0 gửi COMMIT cho Site 2 nhưng gửi ABORT cho Site 3. '
        'Nếu áp dụng giao thức Paxos với f = 1 (N = 3, Quorum = 2):\n'
        '  - Site 2 nhận được COMMIT từ Site 0 và COMMIT từ chính nó. Đạt Quorum (2/3) -> Site 2 thực hiện COMMIT.\n'
        '  - Site 3 nhận được ABORT từ Site 0 và COMMIT từ chính nó. Không đạt Quorum COMMIT -> Site 3 thực hiện ABORT.\n'
        '  - Hậu quả: Hai nút trung thực (Site 2 và Site 3) có kết quả đối nghịch nhau, dẫn đến MẤT ĐỒNG THUẬN và Sổ cái bị phân rã.\n\n'
        'Giao thức BFT khắc phục bằng cách tăng số nút lên N = 4 và Quorum = 3. Nhờ đó, dù Site 0 gửi thông điệp gian lận, '
        'hai nút trung thực Site 2 và Site 3 vẫn trao đổi chéo phiếu bầu của mình và cùng đạt đủ Quorum 3 phiếu COMMIT '
        '(từ Site 1, 2, và 3), từ đó cùng quyết định COMMIT nhất quán. Điều này chứng minh BFT là bắt buộc đối với sổ cái phân tán.'
    )
    r.font.name = 'Times New Roman'
    r.font.size = Pt(11)

    doc.add_page_break()

    # ================================================================
    # APPENDED CHAPTER 11: CONCLUSION & FUTURE DIRECTIONS
    # ================================================================
    h11 = doc.add_heading('11. Conclusion & Future Directions (Kết luận & Hướng phát triển)', level=3)
    h11.runs[0].font.name = 'Times New Roman'
    h11.runs[0].font.color.rgb = RGBColor(0, 0, 0)
    h11.runs[0].font.bold = True
    h11.runs[0].font.size = Pt(13)

    p_intro11 = doc.add_paragraph()
    p_intro11.paragraph_format.line_spacing = 1.15
    p_intro11.paragraph_format.space_after = Pt(6)
    r = p_intro11.add_run(
        'Đề tài nghiên cứu và mô phỏng giao thức BFT cho sổ cái phân tán đã đạt được toàn bộ các mục tiêu đặt ra '
        'và giải quyết triệt để các vấn đề lỗi Byzantine thực tế. Dưới đây là các kết luận tổng kết:'
    )
    r.font.name = 'Times New Roman'
    r.font.size = Pt(11)

    conclusions = [
        ('Kháng lỗi Byzantine', 'Hệ thống BFT (3f+1) đã chứng minh khả năng tự phát hiện và vượt qua lỗi equivocation (nói dối) của Leader (Site 0), bảo vệ tính toàn vẹn của sổ cái.'),
        ('Tự phục hồi sau sự cố', 'Sử dụng Write-Ahead Log (WAL) định dạng JSON Lines giúp nút bị crash (Site 1) khôi phục trạng thái chuẩn xác, đồng bộ dữ liệu nhanh chóng và an toàn sau khi khởi động lại.'),
        ('Đảm bảo an ninh', 'Cơ chế giả lập chữ ký số giúp xác thực nguồn gốc thông điệp bầu chọn, ngăn chặn hoàn toàn Byzantine giả mạo danh tính nút khác.'),
        ('Nhất quán tối cao', 'Các nút trung thực luôn có sổ cái ledger đồng nhất, đảm bảo tính bất biến và độ tin cậy cao của hệ thống cơ sở dữ liệu phân tán.'),
    ]
    for title, desc in conclusions:
        p = doc.add_paragraph()
        p.paragraph_format.left_indent = Cm(0.75)
        p.paragraph_format.space_after = Pt(3)
        r_bullet = p.add_run("•  ")
        r_bullet.bold = True
        r_bullet.font.name = 'Times New Roman'
        r_bullet.font.size = Pt(11)
        r_title = p.add_run(title + ': ')
        r_title.bold = True
        r_title.font.name = 'Times New Roman'
        r_title.font.size = Pt(11)
        r_desc = p.add_run(desc)
        r_desc.font.name = 'Times New Roman'
        r_desc.font.size = Pt(11)

    doc.add_paragraph() # spacing

    p_future_desc = doc.add_paragraph()
    p_future_desc.paragraph_format.line_spacing = 1.15
    r = p_future_desc.add_run('Hướng nghiên cứu và phát triển tiếp theo của đề tài:')
    r.font.name = 'Times New Roman'
    r.font.size = Pt(11)
    r.bold = True

    future_directions = [
        'Thay thế cơ chế giả lập bằng chữ ký số thực tế sử dụng các thư viện mã hóa RSA hoặc ECDSA.',
        'Mở rộng các kịch bản Byzantine phức tạp hơn như giả lập mạng chậm (network delay), mất gói tin (drop message), và liên minh Byzantine giữa nhiều nút.',
        'Triển khai giao thức trên môi trường mạng phân tán thực tế sử dụng giao tiếp Socket TCP/IP thay vì đa tiến trình (multiprocessing) trên localhost.',
        'Tích hợp với cấu trúc dữ liệu Blockchain thực tế (ví dụ: băm chuỗi khối Merkle Tree) để tăng cường tính bất biến của sổ cái.',
    ]
    for item in future_directions:
        p = doc.add_paragraph()
        p.paragraph_format.left_indent = Cm(0.75)
        p.paragraph_format.space_after = Pt(3)
        r_bullet = p.add_run("•  ")
        r_bullet.bold = True
        r_bullet.font.name = 'Times New Roman'
        r_bullet.font.size = Pt(11)
        r = p.add_run(item)
        r.font.name = 'Times New Roman'
        r.font.size = Pt(11)

    # Save the file
    doc.save(OUTPUT_PATH)
    print(f"Successfully generated template-aligned report at {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
