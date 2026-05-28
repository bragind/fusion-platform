cmake_minimum_required(VERSION 3.15)
project(kalman_core)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Если MinGW нуждается в явном флаге (на всякий случай)
if(MINGW)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
endif()

# Подключаем pybind11 и Eigen
add_subdirectory(${CMAKE_SOURCE_DIR}/../../libs/pybind11 pybind11)
include_directories(${CMAKE_SOURCE_DIR}/../../libs/eigen)

# Создаём модуль
pybind11_add_module(kalman_core kalman_core.cpp)