# NightLightsProcessor

NightLightsProcessor is a Python script that downloads, processes, and plots nighttime lights data from an AWS S3 bucket.

## Installation

1. Install Python (version 3.6 or later)
2. Install AWS CLI by following the instructions provided [here](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
3. Install dependencies using pip:

```bash
pip install -r requirements.txt
```

## Usage

```python
# Initialize the NightLightsProcessor object with your AWS S3 bucket name
processor = NightLightsProcessor(bucket_name='globalnightlight')

# Process and plot nighttime lights data for a specific date
ntl_filename = processor.process(date='2024-01-01')
processor.plot(ntl_filename, region_file='SA_regions.json')
```
