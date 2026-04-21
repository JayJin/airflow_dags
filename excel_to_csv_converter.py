import os
import pandas as pd

def convert_excel_to_ansi_csv():
    """
    엑셀 파일을 선택하여 같은 경로의 ANSI(CP949) 형식 CSV로 변환합니다.
    """
    print("=== Excel to ANSI CSV Converter ===")
    
    # 1. 파일 경로 입력 받기
    excel_path = input("변환할 Excel 파일의 전체 경로를 입력하세요: ").strip()
    
    # 따옴표 제거 (경로 복사 시 따옴표가 포함될 수 있음)
    excel_path = excel_path.strip('"').strip("'")
    
    if not os.path.exists(excel_path):
        print(f"Error: 파일을 찾을 수 없습니다: {excel_path}")
        return

    # 2. 파일명 및 확장자 분리
    base_name = os.path.basename(excel_path)
    file_name_no_ext = os.path.splitext(base_name)[0]
    
    # 3. 저장 경로 설정 (현재 파이썬 파일이 위치한 경로)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, f"{file_name_no_ext}.csv")

    try:
        # 4. Excel 파일 읽기
        # openpyxl 엔진이 설치되어 있어야 합니다 (pip install openpyxl)
        print(f"파일 읽는 중: {base_name}...")
        df = pd.read_excel(excel_path)

        # 5. CSV 파일로 저장 (ANSI/CP949 인코딩)
        # index=False 옵션은 행 번호를 제외하고 저장합니다.
        print(f"CSV 변환 중 (ANSI 형식)...")
        df.to_csv(output_path, index=False, encoding='cp949')

        print("-" * 40)
        print(f"성공적으로 변환되었습니다!")
        print(f"저장 위치: {output_path}")
        print("-" * 40)

    except Exception as e:
        print(f"Error: 변환 중 오류가 발생했습니다: {e}")
        if "openpyxl" in str(e).lower():
            print("Tip: 'openpyxl' 라이브러리가 필요합니다. 'pip install openpyxl'을 실행해 주세요.")

if __name__ == "__main__":
    convert_excel_to_ansi_csv()
