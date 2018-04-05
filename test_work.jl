# tests for work helper functions
using Base.Test
include("work.jl")


@testset "split_work tests" begin
    testwork = WorkUnit(10, 3)
    testwork2 = WorkUnit(11, 4)

    @test_throws DomainError split_work(testwork, -0.5)
    @test_throws DomainError split_work(testwork2, 1.63)

    @testset "split_work 50% test" begin
        u1, v1 = split_work(testwork, 0.5)
        u2, v2 = split_work(testwork2, 0.5)
        @test u1.units == 5
        @test v1.units == 5
        @test u2.units == 6
        @test v2.units == 5
    end

    @testset "split_work 1% test" begin
        u1, v1 = split_work(testwork, 0.01)
        u2, v2 = split_work(testwork2, 0.01)
        @test u1.units == 1
        @test v1.units == 9
        @test u2.units == 1
        @test v2.units == 10
    end

end
