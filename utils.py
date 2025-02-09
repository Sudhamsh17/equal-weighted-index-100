import sys
import logging
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                Paragraph, Spacer)


def get_logger(class_name: str, log_level=logging.INFO, log_file_path=None):
    """
    Returns the logger instance created

    Args:
        class_name (str): Logger instance name
        log_level (int, optional): Log level for logging. Defaults to logging.INFO.
        log_file_path (str, optional): log file path to dump the logger logs. Defaults to None.
    """

    if class_name in logging.root.manager.loggerDict:
        return logging.getLogger(class_name)

    logger = logging.getLogger(class_name)
    logger.setLevel(log_level)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(log_level)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_file_path:
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def dump_df_to_pdf(df: pd.DataFrame, pdf_file_path: str, table_header: str) -> None:
    """
    Dumps dataframe to pdf

    Args:
        df (pd.DataFrame): input dataframe
        pdf_file_path (str): path of the output pdf file
        table_header (str): table name
    """

    # PDF File
    pdf = SimpleDocTemplate(pdf_file_path, pagesize=landscape(letter),
                            topMargin=20, bottomMargin=20)

    # Styles
    styles = getSampleStyleSheet()
    title = Paragraph(table_header, styles['Title'])  # Header for the table

    # Convert DataFrame to List
    data = [df.columns.tolist()] + df.values.tolist()

    # Create Table
    table = Table(data)
    style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
    ])
    table.setStyle(style)

    # Build PDF with header and table
    pdf.build([title, Spacer(1, 12), table])  # Spacer adds some space between title and table

