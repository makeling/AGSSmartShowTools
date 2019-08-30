# -*- coding: utf-8 -*-
# !/usr/bin/python
__author__ = 'ma_keling'
# Version     : 1.0.0
# Start Time  : 2019-8-13
# Change Log  :
#   2019-8-29 : Update execute methods for automatic generate vector tile services

import ags_smartshow_tool

def execute_convert_Shps2rasters():
    shp_workspace = '/home/arcgis/arcgis_shps'
    images_workspace = '/home/arcgis/arcgis_raster'
    field_name = 'DLBM'
    resolution = 0.00017166
    proj = 4610

    ags_smartshow_tool.convert_Shps2rasters(shp_workspace, images_workspace, field_name, resolution, proj)

def execute_merge_images():
    draft_workspace = '/gis/raster/draft'
    publish_workspace = '/gis/raster/publish'
    raster_gdb = 'raster.gdb'
    ms_name = 'land_mosaic_2090807'
    prj = '/gis/raster/resources/GCS_Xian_1980.prj'
    origin_rasters_folder = 'land_mosaic_2090807'

    ags_smartshow_tool.merge_image(draft_workspace, publish_workspace, raster_gdb, ms_name, prj, origin_rasters_folder)


def execute_publish_images():
    draft_workspace = '/gis/raster/draft'
    input_raster = '/gis/raster/publish/land_mosaic_2090807.tif'
    con_path = '/gis/raster/resources/server_abi.ags'
    service_name = 'image20190814'

    ags_smartshow_tool.publish_images(draft_workspace, input_raster, con_path, service_name)

def execute_merge_shps():
    target_workspace = '/home/arcgis/arcgis_shps'
    target_shapefile_name = 'target.shp'

    ags_smartshow_tool.merge_shapefiles(target_workspace,target_shapefile_name)

def execute_generate_vtpks():
    import uuid
    # set environment settings

    aprx = "/gis/vt/resources/vt_test.aprx"
    outputPath = "/gis/vt/draft/"
    vtpkname = 'vt_' + str(uuid.uuid1())

    schema = "/gis/vt/resources/VTTS_4610_GCS_Xian_1980.xml"

    db = '/home/arcgis/arcgis_shps/'

    ds = 'target.shp'

    ags_smartshow_tool.replace_datasource(aprx, database=db, dataset=ds)

def execute_publish_vtpks():
    portal_url = "https://abi.arcgisonline.cn/arcgis/home"
    user_name = "arcgis"
    pwd = "******"

    target_workspace = '/gis/vt/draft'

    ags_smartshow_tool.publish_vtpk(portal_url, user_name, pwd, target_workspace)

# workflow: create vector tile package for large scale show
def workflow_generate_vtpk():
    import uuid
    target_workspace = '/home/arcgis/arcgis_shps'
    target_shapefile_name = 'target.shp'

    aprx = "/gis/vt/resources/vt_test.aprx"
    outputPath = "/gis/vt/draft/"
    vtpkname = 'vt_' + str(uuid.uuid1())

    schema = "/gis/vt/resources/VTTS_4610_GCS_Xian_1980.xml"

    # 1 Merge all the shapefiles on each GA node
    ags_smartshow_tool.merge_shapefiles(target_workspace, target_shapefile_name)

    # 2 replace datasource for layer in aprx
    ags_smartshow_tool.replace_datasource(aprx, database=target_workspace, dataset=target_shapefile_name)

    # 3 Generate vtpk on each GA node
    ags_smartshow_tool.create_vtpk(aprx, outputPath, vtpkname, schema)


if __name__ == "__main__":
    # execute_convert_Shps2rasters()
    # execute_merge_images()
    # execute_publish_images()
    workflow_generate_vtpk()
    #execute_publish_vtpks()
