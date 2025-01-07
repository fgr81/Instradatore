# Instradatore

**Instradatore** is a Python project designed to manage the execution of a sequential chain of Python and Bash scripts. Its goal is to simplify workflows by executing scripts in a predefined order, ensuring that dependencies and execution logic are respected.

---

## Features
- **Sequential execution:** Automatically runs Python and Bash scripts in order.
- **Configuration flexibility:** Define execution sequences in a configuration file.
- **Dependencies included:** Utilizes modern Python libraries like `pyyaml` and `rich-click` for enhanced functionality.
- **Support for NetCDF and HDF5:** Includes tools for handling scientific data with `xarray`, `netcdf4`, and `h5netcdf`.
- **Documentation-ready:** Bundled with `Sphinx` and themes for creating beautiful documentation.

---

## Installation

### Prerequisites
- Python **3.10** or later.
- [Poetry](https://python-poetry.org/) for dependency management and project packaging.

### Steps
1. Clone the repository:
   ```bash
   git clone https://github.com/fgr81/instradatore.git
   cd instradatore
   ```

2. Install dependencies using Poetry:
   ```bash
   poetry install
   ```

3. Activate the Poetry virtual environment:
   ```bash
   poetry shell
   ```

---

## Usage
1. Define the execution sequence in a `scripts.yaml` file.
   - Example `ames_34.yaml`

2. Run Instradatore by passing the configuration file:
   ```bash
   python instradatore.py scripts.yaml
   ```

3. The tool will process the scripts in the specified order, displaying real-time progress with clear and colorful output.

---

## Development

### Adding Dependencies
To add a new dependency, use Poetry:
```bash
poetry add <package-name>
```

### Generating Documentation
Instradatore uses **Sphinx** for documentation. To build the docs:
```bash
cd docs
sphinx-build -b html source/ build/
```
The output will be available in the `docs/build/` directory.

---

## Dependencies
Instradatore leverages the following key Python libraries:
- **[pyyaml](https://pypi.org/project/pyyaml/):** For parsing YAML configuration files.
- **[rich-click](https://pypi.org/project/rich-click/):** For enhanced CLI output.
- **[xarray](https://xarray.pydata.org/):** For multidimensional scientific data processing.
- **[netcdf4](https://unidata.github.io/netcdf4-python/):** For working with NetCDF files.
- **[h5netcdf](https://pypi.org/project/h5netcdf/):** For NetCDF compatibility with HDF5.
- **[Sphinx](https://www.sphinx-doc.org/):** For generating documentation.

---

## Contributing
We welcome contributions! To contribute:
1. Fork the repository.
2. Create a branch for your feature or bugfix:
   ```bash
   git checkout -b feature-name
   ```
3. Commit your changes:
   ```bash
   git commit -m "Description of your changes"
   ```
4. Push to your fork:
   ```bash
   git push origin feature-name
   ```
5. Open a Pull Request.

---

## License
This project is licensed under the **GNU GPLv3**. See the [LICENSE](LICENSE) file for details.

---

## Author
Developed by **Fabio Massimo Grasso**  
📧 Email: [fabiomassimo.grasso@cnr.it](mailto:fabiomassimo.grasso@cnr.it)  
🌐 [GitHub Profile](https://github.com/fgr81)
