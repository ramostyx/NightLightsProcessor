import os
import subprocess
import rasterio
from rasterio.merge import merge
import matplotlib.pyplot as plt
import geopandas as gpd
from mpl_toolkits.axes_grid1 import make_axes_locatable
import contextily as cx


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
        command = f"aws s3 ls s3://{self.bucket_name}/{directory_path}/ --no-sign-request --region {self.region}"
        result = self.execute_cli_command(command)

        if result.returncode == 0:
            output_lines = result.stdout.strip().split('\n')

            filtered_object_keys = [line.split()[-1] for line in output_lines if desired_prefix in line]

            print(f"List of objects in the bucket for the year {desired_prefix}:")
            for key in filtered_object_keys:
                print(key)
            return filtered_object_keys
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
        print("combining tiffs")
        tiff_files = [os.path.join(destination_path, file) for file in os.listdir(destination_path) if
                      file.endswith('.tif')]
        src_files_to_mosaic = [rasterio.open(file) for file in tiff_files]
        mosaic, out_trans = merge(src_files_to_mosaic)

        with rasterio.open(output_file, "w", driver="GTiff", width=mosaic.shape[2], height=mosaic.shape[1],
                           count=mosaic.shape[0], dtype=mosaic.dtype, crs=src_files_to_mosaic[0].crs,
                           transform=out_trans) as dst:
            dst.write(mosaic)

        if delete:
            for src in src_files_to_mosaic:
                src.close()
                os.remove(src.name)

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
    ntl_filename = processor.process(date="2024-01-01")
    processor.plot(ntl_filename, region_file="SA_regions.json")
