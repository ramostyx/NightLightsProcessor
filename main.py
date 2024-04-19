import os
import datetime
import subprocess
import rasterio
import rasterio.mask
import matplotlib.pyplot as plt
import geopandas as gpd
from mpl_toolkits.axes_grid1 import make_axes_locatable
import contextily as cx
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed


class NightLightsProcessor:
    def __init__(self, bucket_name, region='us-east-1'):
        self.bucket_name = bucket_name
        self.region = region

    @staticmethod
    def execute_cli_command(command):
        result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return result

    def list_contents(self):
        command = f"aws s3 ls s3://{self.bucket_name}/ --no-sign-request --region {self.region}"
        result = self.execute_cli_command(command)

        if result.returncode == 0:
            output_lines = result.stdout.strip().split('\n')
            object_keys = [line.split()[-1] for line in output_lines]

            print("List of objects in the bucket:")
            for key in object_keys:
                print(key)
        else:
            print(f"Error executing AWS CLI command: {result.stderr}")

    def print_file(self, file_key):
        command = f"aws s3 cp s3://{self.bucket_name}/{file_key} - --no-sign-request"
        result = self.execute_cli_command(command)

        if result.returncode == 0:
            print(f"Content of {file_key}:")
            print(result.stdout)
        else:
            print(f"Error executing AWS CLI command: {result.stderr}")

    def filter_by_prefix(self, desired_prefix, directory_path):
        print(f"Filtering by prefix {desired_prefix}")

        command = f"aws s3 ls s3://{self.bucket_name}/{directory_path}/{desired_prefix} --recursive --no-sign-request --region {self.region}"
        result = self.execute_cli_command(command)

        if result.returncode == 0:
            output_lines = result.stdout.strip().split('\n')
            return [line.split()[-1] for line in output_lines]
        else:
            print(f"Error executing AWS CLI command: {result.stderr}")

    def list_directory_contents(self, directory_path):
        command = f"aws s3 ls s3://{self.bucket_name}/{directory_path}/ --no-sign-request --region {self.region}"
        result = self.execute_cli_command(command)

        if result.returncode == 0:
            output_lines = result.stdout.strip().split('\n')
            object_keys = [line.split()[-1] for line in output_lines]
            print(f"List of objects in the directory '{directory_path}':")
            for key in object_keys:
                print(key)
            return object_keys
        else:
            print(f"Error executing AWS CLI command: {result.stderr}")

    def download_file(self, file_key, destination_path="./"):
        command = f"aws s3 cp s3://{self.bucket_name}/{file_key} {destination_path} --no-sign-request"
        result = self.execute_cli_command(command)

        if result.returncode == 0:
            print(f"Successfully downloaded {file_key} to {destination_path}")
        else:
            print(f"Error downloading {file_key}: {result.stderr}")

    def download_files(self, prefix, folder_name, destination_path="./"):
        if not os.path.exists(destination_path):
            os.makedirs(destination_path)
            command = f'aws s3 cp s3://{self.bucket_name}/{folder_name}/ {destination_path} --recursive --exclude "*" --include "{prefix}*" --no-sign-request'
            result = self.execute_cli_command(command)
            if result.returncode == 0:
                print(f"All files successfully downloaded")
            else:
                print(f"Error downloading {result.stderr}")

    @staticmethod
    def combine_tiff_files(output_file, destination_path, delete=False):
        print("Combining tiffs")
        tiff_files = [os.path.join(destination_path, file) for file in os.listdir(destination_path) if
                      file.endswith('.tif')]

        if not tiff_files:
            print("No TIFF files found to merge.")
            return

        with rasterio.open(tiff_files[0]) as src0:
            meta = src0.meta

        meta.update(count=len(tiff_files))

        with rasterio.open(output_file, 'w', **meta) as dst:
            for idx, file in enumerate(tiff_files):
                with rasterio.open(file) as src:
                    dst.write(src.read(1), idx + 1)

        if delete:
            for file in tiff_files:
                os.remove(file)

    @staticmethod
    def intersected(src, region_bounds):
        from shapely.geometry import box

        raster_polygon = box(*src.bounds)
        region_polygon = box(*region_bounds)

        return raster_polygon.intersects(region_polygon)

    def filter_file_by_region(self, file_key, region_bounds, folder_name):
        s3_path = f"s3://{self.bucket_name}/{file_key}"

        with rasterio.Env(AWS_NO_SIGN_REQUEST="YES"):
            with rasterio.open(s3_path) as src:
                if self.intersected(src, region_bounds):
                    raster_data = src.read(1, window=src.window(*region_bounds))
                    return s3_path, raster_data.sum()
        return None

    def filter_by_region(self, object_keys, region_bounds, folder_name):
        print(f"Filtering by region")
        valid_files = []
        total_files = len(object_keys)
        processed_files = 0
        with ThreadPoolExecutor(total_files) as executor:
            future_to_key = {executor.submit(self.filter_file_by_region, file_key, region_bounds, folder_name): file_key
                             for file_key in object_keys}

            for future in as_completed(future_to_key):
                file_key = future_to_key[future]
                valid_file = future.result()

                processed_files += 1
                print(f"Processed {processed_files}/{total_files} files", end="\r")

                if valid_file is not None:
                    valid_files.append(valid_file)

        return valid_files

    def process(self, date, product_id="SVDNB", spacecraft="npp"):
        folder_name = f"{spacecraft}_{date[:7].replace('-', '')}"
        prefix = f"{product_id}_{spacecraft}_d{date.replace('-', '')}"

        # Download TIFF files
        destination_path = f"./data/{prefix}"
        self.download_files(prefix, folder_name, destination_path=destination_path)

        output_filename = f"{product_id}_{spacecraft}_d{date.replace('-', '')}.tif"
        output_path = os.path.join(destination_path, output_filename)

        if not os.path.exists(output_path):
            self.combine_tiff_files(output_filename, destination_path=destination_path)

        return output_filename

    def calculate_daily_radiation_sum(self, date, region_file, product_id="SVDNB", spacecraft="npp"):
        folder_name = f"{spacecraft}_{date[:7].replace('-', '')}"
        prefix = f"{product_id}_{spacecraft}_d{date.replace('-', '')}"

        radiation_sum = 0.0

        print("Begin filtering")

        object_keys = self.filter_by_prefix(prefix, folder_name)
        region_bounds = gpd.read_file(region_file).total_bounds

        n_workers = 10
        n_files = len(object_keys)
        chunk_size = round(n_files / n_workers)

        with ProcessPoolExecutor(n_workers) as executor:
            futures = list()
            for i in range(0, n_files, chunk_size):
                file_keys = object_keys[i:i + chunk_size]
                future = executor.submit(self.filter_by_region, file_keys, region_bounds, folder_name)
                futures.append(future)

            print("Done filtering")

            for future in as_completed(futures):
                valid_files = future.result()
                for s3_path, rad_sum in valid_files:
                    radiation_sum += rad_sum

        return radiation_sum

    def calculate_monthly_radiation_sum(self, date, region_file, product_id="SVDNB", spacecraft="npp"):

        year, month = map(int, date.split('-'))

        num_days = (datetime.date(year, month + 1, 1) - datetime.date(year, month, 1)).days

        monthly_radiation_sum = 0.0

        with ProcessPoolExecutor(num_days) as executor:
            futures = {executor.submit(self.calculate_daily_radiation_sum, f"{year}-{month:02d}-{day:02d}", region_file,
                                       product_id, spacecraft): f"{year}-{month:02d}-{day:02d}" for day in
                       range(1, num_days + 1)}

            for future in as_completed(futures):
                daily_radiation_sum = future.result()
                monthly_radiation_sum += daily_radiation_sum

        return monthly_radiation_sum

    @staticmethod
    def plot(ntl_file, region_file):
        file_name = ntl_file.split('.')[0]
        polygons_file = gpd.read_file(region_file)
        polygons_file_bbox = polygons_file.total_bounds

        raster_file = rasterio.open(ntl_file)
        raster_file_window = raster_file.window(*polygons_file_bbox)
        raster_file_clipped = raster_file.read(1, window=raster_file_window)

        fig, ax1 = plt.subplots(figsize=(8, 6), dpi=300)
        im1 = ax1.imshow(raster_file_clipped, extent=polygons_file_bbox[[0, 2, 1, 3]], vmin=0, vmax=63, cmap="magma")

        polygons_file.boundary.plot(ax=ax1, color="skyblue", linewidth=0.4)

        cx.add_basemap(ax=ax1, crs=polygons_file.crs.to_string(), source=cx.providers.CartoDB.DarkMatterOnlyLabels)
        divider = make_axes_locatable(ax1)
        cax1 = divider.append_axes("right", size="5%", pad=0.05)
        cbar1 = plt.colorbar(im1, cax=cax1)
        ax1.set_xlabel("Longitude")
        ax1.set_ylabel("Latitude")

        ax1.set_title("Nighttime lights")
        ax1.set_axis_off()

        plt.tight_layout()
        plt.savefig(f"{file_name}.png", dpi=300, bbox_inches="tight")
        plt.show()


# Usage example
if __name__ == "__main__":
    processor = NightLightsProcessor(bucket_name='globalnightlight')
    rad_sum = processor.calculate_daily_radiation_sum(date="2024-01-01", region_file="SA_regions.json")
    print("radiation_sum", rad_sum, "nanowatts/cm2/sr")
    # radiation_sum - 9298689152.0
