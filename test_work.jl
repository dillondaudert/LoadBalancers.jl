# tests for work helper functions
using Base.Test
include("work.jl")


@testset "split_work tests" begin
    testwork = WorkUnit(10, 3)
    testwork2 = WorkUnit(11, 4)
    testwork3 = WorkUnit(1, 1)

    @test_throws DomainError split_work(testwork, -0.5)
    @test_throws DomainError split_work(testwork2, 1.63)
    @test_throws DomainError split_work(testwork3, .5)

    @testset "split_work 50% test" begin
        u1, v1 = split_work(testwork, 0.5)
        u2, v2 = split_work(testwork2, 0.5)
        @test u1.units == 5
        @test v1.units == 5
        @test u2.units == 6
        @test v2.units == 5
    end

    @testset "split_work 0 + ϵ test" begin
        u1, v1 = split_work(testwork, eps(Float64))
        u2, v2 = split_work(testwork2, eps(Float64))
        @test u1.units == 1
        @test v1.units == 9
        @test u2.units == 1
        @test v2.units == 10
    end

    @testset "split_work 1 - ϵ test" begin
        u1, v1 = split_work(testwork, 1 - eps(Float64))
        u2, v2 = split_work(testwork2, 1 - eps(Float64))
        @test u1.units == 9
        @test v1.units == 1
        @test u2.units == 10
        @test v2.units == 1
    end

end
