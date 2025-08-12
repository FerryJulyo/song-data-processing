import oss2
import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog, ttk, scrolledtext, messagebox
from datetime import datetime, timezone
import threading
import queue
import time

# OSS credentials
access_key_id = 'LTAI4G6PxEhqoLDJLvuzdoT6'
access_key_secret = 'P8FJKWmXJImUFGdbHUWKaekDcpJctt'
endpoint = 'oss-ap-southeast-5.aliyuncs.com'
bucket_name = 'happypuppy-prod'

# Initialize OSS
auth = oss2.Auth(access_key_id, access_key_secret)
bucket = oss2.Bucket(auth, endpoint, bucket_name)

class OSSProcessorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("All iCloud Filter")
        self.root.geometry("700x600")
        self.root.resizable(True, True)
        self.root.configure(bg="#f0f0f0")
        
        self.base_path = tk.StringVar(value=os.getcwd())
        self.progress_queue = queue.Queue()
        self.running = False
        
        self.create_widgets()
        self.check_queue()
    
    def create_widgets(self):
        # Base Path Selection
        path_frame = tk.LabelFrame(self.root, text="Base Path", bg="#f0f0f0")
        path_frame.pack(fill="x", padx=20, pady=10)
        
        self.path_entry = tk.Entry(path_frame, textvariable=self.base_path, bd=2, relief="groove")
        self.path_entry.pack(side="left", fill="x", expand=True, padx=5, pady=5)
        
        browse_btn = tk.Button(
            path_frame, 
            text="Browse", 
            command=self.browse_folder,
            bg="white",
            relief="raised",
            bd=2
        )
        browse_btn.pack(side="right", padx=5, pady=5)
        
        # Master File Selection
        master_frame = tk.LabelFrame(self.root, text="Master VOD", bg="#f0f0f0")
        master_frame.pack(fill="x", padx=20, pady=10)
        
        self.master_path = tk.StringVar()
        master_entry = tk.Entry(master_frame, textvariable=self.master_path, bd=2, relief="groove")
        master_entry.pack(side="left", fill="x", expand=True, padx=5, pady=5)
        
        master_btn = tk.Button(
            master_frame, 
            text="Select", 
            command=self.select_master,
            bg="white",
            relief="raised",
            bd=2
        )
        master_btn.pack(side="right", padx=5, pady=5)
        
        # Run Button (centered above progress bar)
        button_frame = tk.Frame(self.root, bg="#f0f0f0")
        button_frame.pack(fill="x", padx=20, pady=15)
        
        self.run_btn = tk.Button(
            button_frame, 
            text="Mulai Proses", 
            command=self.start_processing,
            bg="white",
            fg="black",
            relief="raised",
            bd=2,
            padx=20,
            pady=5,
            font=("Arial", 10, "bold")
        )
        self.run_btn.pack(pady=5)
        
        # Progress Bar
        progress_frame = tk.Frame(self.root, bg="#f0f0f0")
        progress_frame.pack(fill="x", padx=20, pady=(0, 10))
        
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            progress_frame, 
            variable=self.progress_var, 
            maximum=100,
            length=600,
            style="green.Horizontal.TProgressbar"
        )
        self.progress_bar.pack(fill="x", padx=5, pady=5)
        
        self.progress_label = tk.Label(
            progress_frame, 
            text="0%", 
            bg="#f0f0f0",
            font=("Arial", 9)
        )
        self.progress_label.pack(side="right", padx=5)
        
        # Log Area
        log_frame = tk.LabelFrame(self.root, text="Log Proses", bg="#f0f0f0")
        log_frame.pack(fill="both", expand=True, padx=20, pady=10)
        
        self.log_text = scrolledtext.ScrolledText(
            log_frame, 
            state="disabled",
            bg="white",
            bd=2,
            relief="groove"
        )
        self.log_text.pack(fill="both", expand=True, padx=5, pady=5)
        
        # Configure ttk style for rounded corners
        style = ttk.Style()
        style.configure("green.Horizontal.TProgressbar", 
                        thickness=20, 
                        troughcolor='#e0e0e0',
                        background='#4CAF50',
                        troughrelief='flat',
                        borderwidth=1,
                        lightcolor='#4CAF50',
                        darkcolor='#388E3C')
    
    def browse_folder(self):
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            self.base_path.set(folder_selected)
    
    def select_master(self):
        file_path = filedialog.askopenfilename(
            title="Pilih file Master VOD",
            filetypes=[("Excel Files", "*.xlsx *.xls")]
        )
        if file_path:
            self.master_path.set(file_path)
    
    def log_message(self, message):
        try:
            # Pastikan message adalah string dan tidak None
            msg = str(message) if message is not None else ""
            self.log_text.config(state="normal")
            self.log_text.insert("end", msg + "\n")
            self.log_text.see("end")
            self.log_text.config(state="disabled")
        except Exception as e:
            print(f"Error logging message: {e}")  # Fallback ke console jika error

    def check_queue(self):
        try:
            while not self.progress_queue.empty():
                message = self.progress_queue.get()
                
                # Pastikan message tidak None
                if message is None:
                    continue
                    
                if isinstance(message, tuple) and len(message) == 2 and message[0] == "PROSES":
                    self.update_progress(message[1])
                elif message == "ENABLE_BUTTON":
                    self.enable_run_button()
                else:
                    self.log_message(message)
        except Exception as e:
            print(f"Error in queue processing: {e}")
        finally:
            self.root.after(100, self.check_queue)
    
    def update_progress(self, value):
        self.progress_var.set(value)
        self.progress_label.config(text=f"{int(value)}%")
    
    def enable_run_button(self):
        self.run_btn.config(state="normal", bg="white", fg="black")
    
    def disable_run_button(self):
        self.run_btn.config(state="disabled", bg="#cccccc", fg="#888888")
    
    def start_processing(self):
        if not self.master_path.get():
            messagebox.showerror("Error", "Pilih file Master VOD terlebih dahulu!")
            return
        
        if self.running:
            return
            
        # Nonaktifkan tombol run
        self.disable_run_button()
        self.running = True
        self.log_text.config(state="normal")
        self.log_text.delete(1.0, "end")
        self.log_text.config(state="disabled")
        self.update_progress(0)
        
        threading.Thread(
            target=self.run_processing, 
            args=(self.base_path.get(), self.master_path.get()),
            daemon=True
        ).start()
    
    def format_file_info(self, obj):
        last_modified = datetime.fromtimestamp(obj.last_modified, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        size = obj.size
        storage_class = obj.storage_class
        etag = obj.etag
        object_name = f'oss://{bucket_name}/{obj.key}'
        file_name = os.path.basename(obj.key)
        return f'{last_modified};{size};{storage_class};{etag};{object_name};{file_name}'
    
    def list_all_objects(self, prefix, output_file_path, progress_start, progress_end):
        if not self.running:
            return False
            
        self.progress_queue.put(f"Membaca data untuk diproses: {prefix}")
        output_lines = ['LastModifiedTime;Size(B);StorageClass;ETAG;ObjectName;file_name']
        marker = ''
        total_objects = 0
        
        try:
            # First pass to count objects
            self.progress_queue.put("Menghitung jumlah file...")
            while True:
                objs = bucket.list_objects(prefix=prefix, marker=marker)
                total_objects += len(objs.object_list)
                if not objs.next_marker:
                    break
                marker = objs.next_marker
            
            if total_objects == 0:
                self.progress_queue.put(f"Tidak ada data ditemukan: {prefix}")
                return False
                
            # Second pass to process objects
            self.progress_queue.put(f"Memproses {total_objects} file...")
            marker = ''
            processed = 0
            
            while True:
                objs = bucket.list_objects(prefix=prefix, marker=marker)
                for obj in objs.object_list:
                    if not self.running:
                        return False
                    file_info = self.format_file_info(obj)
                    if file_info:  # Pastikan file_info valid
                        output_lines.append(file_info)
                    processed += 1
                    progress = progress_start + (progress_end - progress_start) * (processed / total_objects)
                    self.progress_queue.put(("PROSES", progress))
                    
                if not objs.next_marker:
                    break
                marker = objs.next_marker

            with open(output_file_path, 'w', encoding='utf-8') as f:
                for line in output_lines:
                    if line:  # Pastikan line tidak kosong
                        f.write(line + '\n')
                        
            self.progress_queue.put(f'Berhasil menyimpan file pada {output_file_path}')
            return True
        except Exception as e:
            self.progress_queue.put(f"Error saat list objects: {str(e)}")
            return False

    def txt_to_xlsx(self, file, output, progress_value):
        if not self.running:
            return False
            
        self.progress_queue.put(f"Mengkonversi {file} ke Excel...")
        try:
            df = pd.read_csv(file, sep=';', engine='python')
            filtered_df = df[['ObjectName', 'file_name']]
            filtered_df.to_excel(output, index=False)
            self.progress_queue.put(f'Dikonversi {file} ke {output}')
            self.progress_queue.put(("PROGRESS", progress_value))
            return True
        except Exception as e:
            self.progress_queue.put(f"Error saat proses konversi: {str(e)}")
            return False
    
    def filter_and_save(self, file1, sheet1, file2, sheet2, output_file, progress_value):
        if not self.running:
            return False
            
        self.progress_queue.put(f"Data berhasil disimpan pada {output_file}...")
        try:
            df1 = pd.read_excel(file1, sheet_name=sheet1)
            df2 = pd.read_excel(file2, sheet_name=sheet2)

            df2['FullName'] = df2['SongId'].astype(str).str.strip() + '.' + df2['Format'].astype(str).str.strip()

            df1_clean_names = (
                df1['file_name']
                .dropna()
                .astype(str)
                .str.strip()
                .str.split('.').str[0]
                .str.lstrip('0')
                .str.lower()
                .tolist()
            )

            def is_matched(fullname):
                base = fullname.split('.')[0].strip().lower()
                return any(book_name in base for book_name in df1_clean_names)

            filtered_df = df2[~df2['FullName'].apply(is_matched)]

            result_msg = f'Total lagu di master: {len(df2)}\nDitemukan tidak cocok: {len(filtered_df)}'
            self.progress_queue.put(result_msg)

            filtered_df.to_excel(output_file, index=False)
            self.progress_queue.put(f'Berhasil menyimpan file pada {output_file}')
            self.progress_queue.put(("PROSES", progress_value))
            return True
        except Exception as e:
            self.progress_queue.put(f"Error saat proses filterisasi: {str(e)}")
            return False
    
    def run_processing(self, base_path, master_file):
        try:
            today_str = datetime.now().strftime('%Y%m%d')
            os.makedirs(base_path, exist_ok=True)
            
            # Step 1: List objects for 'OKE/song-file/'
            oke_txt_path = os.path.join(base_path, f'{today_str}_oke.txt')
            if not self.list_all_objects('OKE/song-file/', oke_txt_path, 0, 25):
                raise Exception("Gagal memproses OKE/song-file/")
                
            # Step 2: List objects for 'song-file/'
            notoke_txt_path = os.path.join(base_path, f'{today_str}.txt')
            if not self.list_all_objects('song-file/', notoke_txt_path, 25, 50):
                raise Exception("Gagal memproses song-file/")
            
            # Step 3: Convert txt to xlsx
            oke_xlsx_path = os.path.join(base_path, f'{today_str}_oke.xlsx')
            if not self.txt_to_xlsx(oke_txt_path, oke_xlsx_path, 60):
                raise Exception("Gagal konversi OKE ke Excel")
                
            notoke_xlsx_path = os.path.join(base_path, f'{today_str}.xlsx')
            if not self.txt_to_xlsx(notoke_txt_path, notoke_xlsx_path, 70):
                raise Exception("Gagal konversi not-OKE ke Excel")
            
            # Step 4: Filter data
            sheet_master = 'Song'
            output_oke = os.path.join(base_path, f'{today_str}_filtered_data_OKE.xlsx')
            if not self.filter_and_save(oke_xlsx_path, 'Sheet1', master_file, sheet_master, output_oke, 80):
                raise Exception("Gagal filter data OKE")
                
            output_notoke = os.path.join(base_path, f'{today_str}_filtered_data.xlsx')
            if not self.filter_and_save(notoke_xlsx_path, 'Sheet1', master_file, sheet_master, output_notoke, 100):
                raise Exception("Gagal filter data not-OKE")
            
            self.progress_queue.put("Proses selesai dengan sukses!")
        except Exception as e:
            self.progress_queue.put(f"Error dalam proses utama: {str(e)}")
        finally:
            self.running = False
            self.progress_queue.put("ENABLE_BUTTON")

if __name__ == "__main__":
    root = tk.Tk()
    app = OSSProcessorApp(root)
    root.mainloop()